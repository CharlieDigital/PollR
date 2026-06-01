using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using ZLinq;

namespace PollR;

/// <summary>
/// Shared-feed subscriber and cursor state for the existing poller implementation.
/// </summary>
/// <typeparam name="TData">The type of data being processed.</typeparam>
/// <typeparam name="TPartition">The type of partition key.</typeparam>
/// <typeparam name="TCursor">The type of cursor used for tracking positions.</typeparam>
/// <remarks>
/// This type intentionally does not own runner lifetime. The lifecycle base now owns
/// cancellation and tick orchestration; the context stays focused on shared-feed
/// subscriber registration, cursor selection, and catch-up promotion.
/// </remarks>
/// <param name="initialCursorFactory">A factory function to provide the initial cursor position.</param>
/// <param name="clampCursor">A function to clamp cursor positions within valid bounds.</param>
internal sealed class PollRContext<TData, TPartition, TCursor>(
    Func<TCursor> initialCursorFactory,
    Func<TCursor, TCursor> clampCursor
)
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    TCursor? _cursorPosition;
    TCursor? _activeCatchUpCursorPosition;
    bool _hasCursorPosition;
    bool _hasActiveCatchUpCursorPosition;

    readonly ConcurrentDictionary<
        TPartition,
        PartitionSubscribers<TData, TPartition, TCursor>
    > _subscribers = [];

    readonly ConcurrentDictionary<
        long,
        PendingSubscriber<TData, TPartition, TCursor>
    > _pendingSubscribers = [];

    readonly Channel<PendingSubscriber<TData, TPartition, TCursor>> _subscriberJoins =
        Channel.CreateUnbounded<PendingSubscriber<TData, TPartition, TCursor>>();

    long _nextSubscriberId;
    public IEnumerable<Subscriber<TData, TPartition, TCursor>> Subscribers => GetSubscribers();

    public void Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        IDataStream<IntervalData<TData, TPartition, TCursor>> stream,
        CancellationToken cancellationToken
    )
    {
        var subscriberId = Interlocked.Increment(ref _nextSubscriberId);
        var subscriberCursorPosition = ClampCursorPosition(cursorPosition);
        var subscriber = new Subscriber<TData, TPartition, TCursor>(
            subscriberCursorPosition,
            stream
        );
        var pendingSubscriber = new PendingSubscriber<TData, TPartition, TCursor>(
            subscriberId,
            partition,
            subscriber
        );

        EnqueueSubscriberJoin(pendingSubscriber);

        subscriber.CancellationRegistration = cancellationToken.Register(
            static state =>
            {
                var (owner, subscriberPartition, id) = ((
                    PollRContext<TData, TPartition, TCursor>,
                    TPartition,
                    long
                ))
                    state!;
                owner.RemoveSubscriber(
                    subscriberPartition,
                    id,
                    disposeCancellationRegistration: false
                );
            },
            (this, partition, subscriberId)
        );
    }

    public bool TryGetPartitionSubscribers(
        TPartition partition,
        [NotNullWhen(true)] out PartitionSubscribers<TData, TPartition, TCursor>? subscribers
    ) => _subscribers.TryGetValue(partition, out subscribers);

    public TCursor GetNextCursorPosition() =>
        ClampCursorPosition(
            _hasActiveCatchUpCursorPosition
                ? _activeCatchUpCursorPosition!
                : GetCurrentCursorPosition()
        );

    public TCursor ClampCursorPosition(TCursor cursorPosition) => clampCursor(cursorPosition);

    public void CompletePollingTick(TCursor cursorPosition)
    {
        _cursorPosition = cursorPosition;
        _hasCursorPosition = true;

        foreach (var subscribers in _subscribers.Values.AsValueEnumerable())
        {
            subscribers.PromoteCaughtUp(cursorPosition);
        }

        _activeCatchUpCursorPosition = default;
        _hasActiveCatchUpCursorPosition = false;
    }

    public void RegisterPendingSubscribers()
    {
        while (_subscriberJoins.Reader.TryRead(out var pendingSubscriber))
        {
            if (
                !_pendingSubscribers.TryRemove(
                    pendingSubscriber.SubscriberId,
                    out var removedPendingSubscriber
                )
            )
            {
                continue;
            }

            var subscriber = removedPendingSubscriber.Subscriber;
            var currentCursorPosition = GetCurrentCursorPosition();
            var partitionSubscribers = _subscribers.GetOrAdd(
                removedPendingSubscriber.Partition,
                _ => new PartitionSubscribers<TData, TPartition, TCursor>()
            );

            partitionSubscribers.Add(
                removedPendingSubscriber.SubscriberId,
                subscriber,
                isCatchingUp: subscriber.CursorPosition.CompareTo(currentCursorPosition) < 0
            );

            if (
                subscriber.CursorPosition.CompareTo(currentCursorPosition) < 0
                && (
                    !_hasActiveCatchUpCursorPosition
                    || subscriber.CursorPosition.CompareTo(_activeCatchUpCursorPosition!) < 0
                )
            )
            {
                _activeCatchUpCursorPosition = subscriber.CursorPosition;
                _hasActiveCatchUpCursorPosition = true;
            }
        }
    }

    public void CompleteSubscribers()
    {
        foreach (var (subscriberId, pendingSubscriber) in _pendingSubscribers.AsValueEnumerable())
        {
            if (_pendingSubscribers.TryRemove(subscriberId, out _))
            {
                pendingSubscriber.Subscriber.Complete(disposeCancellationRegistration: true);
            }
        }

        foreach (var subscriber in Subscribers)
        {
            subscriber.Complete(disposeCancellationRegistration: true);
        }

        _subscribers.Clear();
    }

    private void EnqueueSubscriberJoin(
        PendingSubscriber<TData, TPartition, TCursor> pendingSubscriber
    )
    {
        _pendingSubscribers[pendingSubscriber.SubscriberId] = pendingSubscriber;
        _subscriberJoins.Writer.TryWrite(pendingSubscriber);
    }

    private void RemoveSubscriber(
        TPartition partition,
        long subscriberId,
        bool disposeCancellationRegistration
    )
    {
        if (_pendingSubscribers.TryRemove(subscriberId, out var pendingSubscriber))
        {
            pendingSubscriber.Subscriber.Complete(disposeCancellationRegistration);
            return;
        }

        if (!_subscribers.TryGetValue(partition, out var partitionSubscribers))
        {
            return;
        }

        if (!partitionSubscribers.TryRemove(subscriberId, out var subscriber) || subscriber is null)
        {
            return;
        }

        subscriber.Complete(disposeCancellationRegistration);

        if (partitionSubscribers.IsEmpty)
        {
            _subscribers.TryRemove(partition, out _);
        }
    }

    public TCursor GetCurrentCursorPosition() =>
        _hasCursorPosition ? _cursorPosition! : initialCursorFactory();

    private IEnumerable<Subscriber<TData, TPartition, TCursor>> GetSubscribers()
    {
        foreach (var partitionSubscribers in _subscribers.Values)
        {
            foreach (var subscriber in partitionSubscribers.All)
            {
                yield return subscriber;
            }
        }
    }
}
