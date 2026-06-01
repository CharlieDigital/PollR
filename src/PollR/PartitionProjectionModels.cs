using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using ZLinq;

namespace PollR;

/// <summary>
/// Internal contract for partition-aware projection groups.
/// </summary>
internal interface IPartitionProjectionGroup<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    Type PayloadType { get; }

    void RegisterPendingSubscribers();

    bool TryGetNextCursorPosition(
        TPartition partition,
        [NotNullWhen(true)] out TCursor? cursorPosition
    );

    void CompletePollingTick(TPartition partition, TCursor cursorPosition);

    ValueTask ProjectAndBroadcastAsync(
        ProducerResult<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    );

    ValueTask BroadcastErrorAsync(
        TPartition partition,
        ErrorResult error,
        CancellationToken cancellationToken
    );

    ValueTask BroadcastCompletedAsync(
        StreamCompletedResult completed,
        CancellationToken cancellationToken
    );

    void CompleteSubscribers();
}

/// <summary>
/// Partition-aware projection group for one registered key.
/// </summary>
internal sealed class PartitionProjectionGroup<TData, TPartition, TCursor, TPayload>(
    Func<DataResult<TData, TPartition, TCursor>, TPayload> project,
    PartitionedPollRContext<TData, TPartition, TCursor> context
) : IPartitionProjectionGroup<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    readonly PartitionProjectionSubscriptionRegistry<TPayload, TPartition, TCursor> _subscriptions =
        new(
            partition => context.TrackPendingProjectionSubscriber(partition),
            partition => context.PromotePendingProjectionSubscriber(partition),
            (partition, wasPending) => context.RemoveProjectionSubscriber(partition, wasPending),
            partition => context.GetCurrentCursorPosition(partition)
        );

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

    public void RegisterPendingSubscribers() => _subscriptions.RegisterPendingSubscribers();

    public bool TryGetNextCursorPosition(
        TPartition partition,
        [NotNullWhen(true)] out TCursor? cursorPosition
    ) => _subscriptions.TryGetNextCursorPosition(partition, out cursorPosition);

    public void CompletePollingTick(TPartition partition, TCursor cursorPosition) =>
        _subscriptions.CompletePollingTick(partition, cursorPosition);

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
                // Callout: one projection execution per item/key/partition, then fan out.
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
        TPartition partition,
        ErrorResult error,
        CancellationToken cancellationToken
    )
    {
        if (!_subscriptions.TryGetPartitionSubscribers(partition, out var subscribers))
        {
            return;
        }

        IntervalData<TPayload, TPartition, TCursor> item = error;

        foreach (var subscriber in subscribers.All)
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

/// <summary>
/// Partition-aware projection subscriber registry.
/// </summary>
internal sealed class PartitionProjectionSubscriptionRegistry<TPayload, TPartition, TCursor>(
    Action<TPartition> onPendingSubscriberEnqueued,
    Action<TPartition> onPendingSubscriberRegistered,
    Action<TPartition, bool> onSubscriberRemoved,
    Func<TPartition, TCursor> getCurrentCursorPosition
)
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

    readonly ConcurrentDictionary<TPartition, TCursor> _activeCatchUpCursorPositions = [];

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
        onPendingSubscriberEnqueued(partition);

        subscriber.CancellationRegistration = cancellationToken.Register(
            static state =>
            {
                var (owner, subscriberPartition, id) = ((
                    PartitionProjectionSubscriptionRegistry<TPayload, TPartition, TCursor>,
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
            var partitionSubscribers = _subscribers.GetOrAdd(
                removedPendingSubscriber.Partition,
                _ => new PartitionSubscribers<TPayload, TPartition, TCursor>()
            );
            var currentCursorPosition = getCurrentCursorPosition(
                removedPendingSubscriber.Partition
            );
            var isCatchingUp = subscriber.CursorPosition.CompareTo(currentCursorPosition) < 0;

            partitionSubscribers.Add(
                removedPendingSubscriber.SubscriberId,
                subscriber,
                isCatchingUp
            );

            if (isCatchingUp)
            {
                _activeCatchUpCursorPositions.AddOrUpdate(
                    removedPendingSubscriber.Partition,
                    subscriber.CursorPosition,
                    (_, existingCursor) =>
                        subscriber.CursorPosition.CompareTo(existingCursor) < 0
                            ? subscriber.CursorPosition
                            : existingCursor
                );
            }

            onPendingSubscriberRegistered(removedPendingSubscriber.Partition);
        }
    }

    public bool TryGetNextCursorPosition(
        TPartition partition,
        [NotNullWhen(true)] out TCursor? cursorPosition
    ) => _activeCatchUpCursorPositions.TryGetValue(partition, out cursorPosition);

    public bool TryGetPartitionSubscribers(
        TPartition partition,
        [NotNullWhen(true)] out PartitionSubscribers<TPayload, TPartition, TCursor>? subscribers
    ) => _subscribers.TryGetValue(partition, out subscribers);

    public void CompletePollingTick(TPartition partition, TCursor cursorPosition)
    {
        if (_subscribers.TryGetValue(partition, out var subscribers))
        {
            subscribers.PromoteCaughtUp(cursorPosition);
        }

        _activeCatchUpCursorPositions.TryRemove(partition, out _);
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
        _activeCatchUpCursorPositions.Clear();
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
            onSubscriberRemoved(partition, true);
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
        onSubscriberRemoved(partition, false);

        if (partitionSubscribers.IsEmpty)
        {
            _subscribers.TryRemove(partition, out _);
            _activeCatchUpCursorPositions.TryRemove(partition, out _);
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
