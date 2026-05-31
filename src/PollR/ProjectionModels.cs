using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using ZLinq;

namespace PollR;

internal interface IProjectionGroup<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    Type PayloadType { get; }

    void RegisterPendingSubscribers(TCursor currentCursorPosition);

    bool TryGetNextCursorPosition([NotNullWhen(true)] out TCursor? cursorPosition);

    void CompletePollingTick(TCursor cursorPosition);

    ValueTask ProjectAndBroadcastAsync(
        ProducerResult<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    );

    ValueTask BroadcastErrorAsync(ErrorResult error, CancellationToken cancellationToken);

    ValueTask BroadcastCompletedAsync(
        StreamCompletedResult completed,
        CancellationToken cancellationToken
    );

    void CompleteSubscribers();
}

internal sealed class ProjectionGroup<TData, TPartition, TCursor, TPayload>(
    Func<DataResult<TData, TPartition, TCursor>, TPayload> project
) : IProjectionGroup<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    readonly ProjectionSubscriptionRegistry<TPayload, TPartition, TCursor> _subscriptions = new();

    public Type PayloadType => typeof(TPayload);

    public ChannelDataStream<IntervalData<TPayload, TPartition, TCursor>> Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken
    )
    {
        var stream = ChannelDataStream<
            IntervalData<TPayload, TPartition, TCursor>
        >.CreateBoundedDropWrite(streamCapacity);
        _subscriptions.Subscribe(partition, cursorPosition, stream, cancellationToken);
        return stream;
    }

    public void RegisterPendingSubscribers(TCursor currentCursorPosition) =>
        _subscriptions.RegisterPendingSubscribers(currentCursorPosition);

    public bool TryGetNextCursorPosition([NotNullWhen(true)] out TCursor? cursorPosition) =>
        _subscriptions.TryGetNextCursorPosition(out cursorPosition);

    public void CompletePollingTick(TCursor cursorPosition) =>
        _subscriptions.CompletePollingTick(cursorPosition);

    public async ValueTask ProjectAndBroadcastAsync(
        ProducerResult<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    )
    {
        if (!_subscriptions.TryGetPartitionSubscribers(item.Partition, out var subscribers))
        {
            return;
        }

        var hasProjectedResult = false;
        IntervalData<TPayload, TPartition, TCursor> projectedResult = default;

        foreach (var subscriber in subscribers.All)
        {
            if (!subscriber.TryAdvanceCursor(item.Cursor))
            {
                continue;
            }

            if (!hasProjectedResult)
            {
                // The first subscriber that needs this record pays the projection cost.
                // Every other subscriber in the same partition/projection group receives
                // the same IntervalData instance for this broadcast.
                projectedResult = new DataResult<TPayload, TPartition, TCursor>(
                    project(
                        new DataResult<TData, TPartition, TCursor>(
                            item.Data,
                            item.Cursor,
                            item.Partition
                        )
                    ),
                    item.Cursor,
                    item.Partition
                );
                hasProjectedResult = true;
            }

            await PushAsync(subscriber, projectedResult, cancellationToken);
        }
    }

    public async ValueTask BroadcastErrorAsync(
        ErrorResult error,
        CancellationToken cancellationToken
    )
    {
        IntervalData<TPayload, TPartition, TCursor> item = error;

        foreach (var subscriber in _subscriptions.Subscribers)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await PushAsync(subscriber, item, cancellationToken);
        }
    }

    public async ValueTask BroadcastCompletedAsync(
        StreamCompletedResult completed,
        CancellationToken cancellationToken
    )
    {
        IntervalData<TPayload, TPartition, TCursor> item = completed;

        foreach (var subscriber in _subscriptions.Subscribers)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await PushAsync(subscriber, item, cancellationToken);
        }
    }

    public void CompleteSubscribers() => _subscriptions.CompleteSubscribers();

    static async ValueTask PushAsync(
        Subscriber<TPayload, TPartition, TCursor> subscriber,
        IntervalData<TPayload, TPartition, TCursor> item,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await subscriber.Stream.PushAsync(item, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (InvalidOperationException) { }
    }
}

internal sealed class ProjectionSubscriptionRegistry<TPayload, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    readonly ConcurrentDictionary<
        TPartition,
        PartitionSubscribers<TPayload, TPartition, TCursor>
    > _subscribers = [];

    readonly ConcurrentDictionary<
        long,
        PendingSubscriber<TPayload, TPartition, TCursor>
    > _pendingSubscribers = [];

    readonly Channel<PendingSubscriber<TPayload, TPartition, TCursor>> _subscriberJoins =
        Channel.CreateUnbounded<PendingSubscriber<TPayload, TPartition, TCursor>>();

    TCursor? _activeCatchUpCursorPosition;
    bool _hasActiveCatchUpCursorPosition;
    long _nextSubscriberId;

    public IEnumerable<Subscriber<TPayload, TPartition, TCursor>> Subscribers => GetSubscribers();

    public void Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        IDataStream<IntervalData<TPayload, TPartition, TCursor>> stream,
        CancellationToken cancellationToken
    )
    {
        var subscriberId = Interlocked.Increment(ref _nextSubscriberId);
        var subscriber = new Subscriber<TPayload, TPartition, TCursor>(cursorPosition, stream);
        var pendingSubscriber = new PendingSubscriber<TPayload, TPartition, TCursor>(
            subscriberId,
            partition,
            subscriber
        );

        _pendingSubscribers[subscriberId] = pendingSubscriber;
        _subscriberJoins.Writer.TryWrite(pendingSubscriber);

        subscriber.CancellationRegistration = cancellationToken.Register(
            static state =>
            {
                var (owner, subscriberPartition, id) = ((
                    ProjectionSubscriptionRegistry<TPayload, TPartition, TCursor>,
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

    public void RegisterPendingSubscribers(TCursor currentCursorPosition)
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
            var partitionSubscribers = _subscribers.GetOrAdd(
                removedPendingSubscriber.Partition,
                _ => new PartitionSubscribers<TPayload, TPartition, TCursor>()
            );
            var isCatchingUp = subscriber.CursorPosition.CompareTo(currentCursorPosition) < 0;

            partitionSubscribers.Add(
                removedPendingSubscriber.SubscriberId,
                subscriber,
                isCatchingUp
            );

            if (
                isCatchingUp
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

    public bool TryGetNextCursorPosition([NotNullWhen(true)] out TCursor? cursorPosition)
    {
        cursorPosition = _activeCatchUpCursorPosition;
        return _hasActiveCatchUpCursorPosition;
    }

    public bool TryGetPartitionSubscribers(
        TPartition partition,
        [NotNullWhen(true)] out PartitionSubscribers<TPayload, TPartition, TCursor>? subscribers
    ) => _subscribers.TryGetValue(partition, out subscribers);

    public void CompletePollingTick(TCursor cursorPosition)
    {
        foreach (var subscribers in _subscribers.Values.AsValueEnumerable())
        {
            subscribers.PromoteCaughtUp(cursorPosition);
        }

        _activeCatchUpCursorPosition = default;
        _hasActiveCatchUpCursorPosition = false;
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

    void RemoveSubscriber(
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

    IEnumerable<Subscriber<TPayload, TPartition, TCursor>> GetSubscribers()
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
