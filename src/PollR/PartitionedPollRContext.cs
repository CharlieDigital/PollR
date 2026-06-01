using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using ZLinq;

namespace PollR;

/// <summary>
/// Owns partition registry state for the partitioned poller.
/// </summary>
/// <remarks>
/// This keeps direct-subscriber intake global while making cursor, retry, and catch-up
/// state partition-local. All structural mutations flow through one lock so fairness
/// order and active-partition membership stay easy to reason about.
/// </remarks>
internal sealed class PartitionedPollRContext<TData, TPartition, TCursor>(
    TimeSpan pollingInterval,
    Func<TCursor> initialCursorFactory,
    Func<TCursor, TCursor> clampCursor
)
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    readonly ConcurrentDictionary<
        TPartition,
        PartitionFeedState<TData, TPartition, TCursor>
    > _partitions = [];

    readonly ConcurrentDictionary<
        long,
        PendingSubscriber<TData, TPartition, TCursor>
    > _pendingSubscribers = [];

    readonly Channel<PendingSubscriber<TData, TPartition, TCursor>> _subscriberJoins =
        Channel.CreateUnbounded<PendingSubscriber<TData, TPartition, TCursor>>();

    readonly Lock _stateLock = new();
    readonly List<TPartition> _partitionOrder = [];
    readonly EqualityComparer<TPartition> _partitionComparer = EqualityComparer<TPartition>.Default;

    int _scanStartIndex;
    long _nextSubscriberId;

    /// <summary>
    /// Global polling interval shared by all partitions in v1.
    /// </summary>
    public TimeSpan PollingInterval { get; } = pollingInterval;

    /// <summary>
    /// Visible active subscribers across all active partitions.
    /// </summary>
    public IEnumerable<Subscriber<TData, TPartition, TCursor>> Subscribers => GetSubscribers();

    /// <summary>
    /// Stages a direct subscriber join without mutating active partition state.
    /// </summary>
    public void Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        IDataStream<IntervalData<TData, TPartition, TCursor>> stream,
        CancellationToken cancellationToken
    )
    {
        var subscriberId = Interlocked.Increment(ref _nextSubscriberId);
        var subscriber = new Subscriber<TData, TPartition, TCursor>(
            ClampCursorPosition(cursorPosition),
            stream
        );
        var pendingSubscriber = new PendingSubscriber<TData, TPartition, TCursor>(
            subscriberId,
            partition,
            subscriber
        );

        // Callout: joins stay staged until the next tick boundary.
        _pendingSubscribers[subscriberId] = pendingSubscriber;
        _subscriberJoins.Writer.TryWrite(pendingSubscriber);

        subscriber.CancellationRegistration = cancellationToken.Register(
            static state =>
            {
                var (owner, subscriberPartition, id) = ((
                    PartitionedPollRContext<TData, TPartition, TCursor>,
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

    /// <summary>
    /// Registers all staged direct subscribers into their partition state.
    /// </summary>
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

            lock (_stateLock)
            {
                var partitionState = GetOrCreatePartitionState(removedPendingSubscriber.Partition);

                partitionState.AddSubscriber(
                    removedPendingSubscriber.SubscriberId,
                    removedPendingSubscriber.Subscriber,
                    initialCursorFactory
                );
            }
        }
    }

    /// <summary>
    /// Returns a rotated snapshot of due partitions and marks them in-flight.
    /// </summary>
    public IReadOnlyList<PartitionFeedState<TData, TPartition, TCursor>> GetDuePartitions(
        DateTimeOffset utcNow
    )
    {
        lock (_stateLock)
        {
            if (_partitionOrder.Count == 0)
            {
                return [];
            }

            var duePartitions = new List<PartitionFeedState<TData, TPartition, TCursor>>(
                _partitionOrder.Count
            );

            for (var offset = 0; offset < _partitionOrder.Count; offset++)
            {
                var partition = _partitionOrder[(_scanStartIndex + offset) % _partitionOrder.Count];

                if (!_partitions.TryGetValue(partition, out var partitionState))
                {
                    continue;
                }

                // Callout: once selected for this tick, the partition is marked polling
                // under the same lock so it cannot be selected or removed concurrently.
                if (partitionState.IsPolling || partitionState.NextDueAt > utcNow)
                {
                    continue;
                }

                partitionState.IsPolling = true;
                duePartitions.Add(partitionState);
            }

            return duePartitions;
        }
    }

    /// <summary>
    /// Advances the rotating fairness anchor once per tick.
    /// </summary>
    public void AdvanceScanStartIndex()
    {
        lock (_stateLock)
        {
            if (_partitionOrder.Count == 0)
            {
                _scanStartIndex = 0;
                return;
            }

            _scanStartIndex = (_scanStartIndex + 1) % _partitionOrder.Count;
        }
    }

    /// <summary>
    /// Computes the next background-loop delay based on active partition due times.
    /// </summary>
    public TimeSpan GetDelayUntilNextDue(DateTimeOffset utcNow)
    {
        lock (_stateLock)
        {
            if (_partitionOrder.Count == 0)
            {
                return PollingInterval;
            }

            var hasCandidate = false;
            var nextDelay = PollingInterval;

            foreach (var partition in _partitionOrder)
            {
                if (
                    !_partitions.TryGetValue(partition, out var partitionState)
                    || partitionState.IsPolling
                )
                {
                    continue;
                }

                if (partitionState.NextDueAt <= utcNow)
                {
                    return TimeSpan.Zero;
                }

                var candidateDelay = partitionState.NextDueAt - utcNow;

                if (!hasCandidate || candidateDelay < nextDelay)
                {
                    nextDelay = candidateDelay;
                    hasCandidate = true;
                }
            }

            return hasCandidate ? nextDelay : PollingInterval;
        }
    }

    /// <summary>
    /// Marks a completed partition poll and removes the partition immediately when it is idle.
    /// </summary>
    public void CompletePartitionPoll(TPartition partition)
    {
        lock (_stateLock)
        {
            if (!_partitions.TryGetValue(partition, out var partitionState))
            {
                return;
            }

            partitionState.IsPolling = false;

            if (
                partitionState.IsIdle
                && !partitionState.HasPendingProjectionSubscribers
                && !HasPendingSubscriberForPartition(partition)
            )
            {
                RemovePartitionState(partition);
            }
        }
    }

    /// <summary>
    /// Completes all visible and pending direct subscribers.
    /// </summary>
    public void CompleteSubscribers()
    {
        foreach (var (subscriberId, pendingSubscriber) in _pendingSubscribers.AsValueEnumerable())
        {
            if (_pendingSubscribers.TryRemove(subscriberId, out _))
            {
                pendingSubscriber.Subscriber.Complete(disposeCancellationRegistration: true);
            }
        }

        foreach (var partitionState in _partitions.Values.AsValueEnumerable())
        {
            partitionState.CompleteSubscribers();
        }

        lock (_stateLock)
        {
            _partitions.Clear();
            _partitionOrder.Clear();
            _scanStartIndex = 0;
        }
    }

    /// <summary>
    /// Ensures partition state exists for a staged projection subscriber.
    /// </summary>
    public void TrackPendingProjectionSubscriber(TPartition partition)
    {
        lock (_stateLock)
        {
            var partitionState = GetOrCreatePartitionState(partition);
            partitionState.AddPendingProjectionSubscriber();
        }
    }

    /// <summary>
    /// Promotes a staged projection subscriber into the active projection set.
    /// </summary>
    public void PromotePendingProjectionSubscriber(TPartition partition)
    {
        lock (_stateLock)
        {
            var partitionState = GetOrCreatePartitionState(partition);
            partitionState.PromotePendingProjectionSubscriber();
        }
    }

    /// <summary>
    /// Removes a projection subscriber from either the pending or active set.
    /// </summary>
    public void RemoveProjectionSubscriber(TPartition partition, bool wasPending)
    {
        lock (_stateLock)
        {
            if (!_partitions.TryGetValue(partition, out var partitionState))
            {
                return;
            }

            partitionState.RemoveProjectionSubscriber(wasPending);

            if (
                !partitionState.IsPolling
                && partitionState.IsIdle
                && !partitionState.HasPendingProjectionSubscribers
                && !HasPendingSubscriberForPartition(partition)
            )
            {
                RemovePartitionState(partition);
            }
        }
    }

    /// <summary>
    /// Returns the current cursor for a partition, creating the state if needed.
    /// </summary>
    public TCursor GetCurrentCursorPosition(TPartition partition)
    {
        lock (_stateLock)
        {
            return GetOrCreatePartitionState(partition)
                .GetCurrentCursorPosition(initialCursorFactory);
        }
    }

    /// <summary>
    /// Clamps one cursor request using the configured cursor policy.
    /// </summary>
    public TCursor ClampCursorPosition(TCursor cursorPosition) => clampCursor(cursorPosition);

    /// <summary>
    /// Computes the next query cursor for a partition.
    /// </summary>
    public TCursor GetNextCursorPosition(
        PartitionFeedState<TData, TPartition, TCursor> partitionState
    ) => partitionState.GetNextCursorPosition(initialCursorFactory, clampCursor);

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

        lock (_stateLock)
        {
            if (!_partitions.TryGetValue(partition, out var partitionState))
            {
                return;
            }

            if (
                !partitionState.TryRemoveSubscriber(subscriberId, out var subscriber)
                || subscriber is null
            )
            {
                return;
            }

            subscriber.Complete(disposeCancellationRegistration);

            if (
                !partitionState.IsPolling
                && partitionState.IsIdle
                && !partitionState.HasPendingProjectionSubscribers
                && !HasPendingSubscriberForPartition(partition)
            )
            {
                RemovePartitionState(partition);
            }
        }
    }

    PartitionFeedState<TData, TPartition, TCursor> GetOrCreatePartitionState(TPartition partition)
    {
        if (_partitions.TryGetValue(partition, out var existingState))
        {
            return existingState;
        }

        var partitionState = new PartitionFeedState<TData, TPartition, TCursor>(partition);
        _partitions[partition] = partitionState;
        _partitionOrder.Add(partition);
        return partitionState;
    }

    bool HasPendingSubscriberForPartition(TPartition partition)
    {
        foreach (var pendingSubscriber in _pendingSubscribers.Values)
        {
            if (_partitionComparer.Equals(pendingSubscriber.Partition, partition))
            {
                return true;
            }
        }

        return false;
    }

    void RemovePartitionState(TPartition partition)
    {
        if (!_partitions.TryRemove(partition, out _))
        {
            return;
        }

        var removedIndex = _partitionOrder.FindIndex(candidate =>
            _partitionComparer.Equals(candidate, partition)
        );

        if (removedIndex < 0)
        {
            return;
        }

        _partitionOrder.RemoveAt(removedIndex);

        if (_partitionOrder.Count == 0)
        {
            _scanStartIndex = 0;
            return;
        }

        if (removedIndex < _scanStartIndex)
        {
            _scanStartIndex--;
        }

        if (_scanStartIndex >= _partitionOrder.Count)
        {
            _scanStartIndex = 0;
        }
    }

    IEnumerable<Subscriber<TData, TPartition, TCursor>> GetSubscribers()
    {
        foreach (var partitionState in _partitions.Values)
        {
            foreach (var subscriber in partitionState.Subscribers.All)
            {
                yield return subscriber;
            }
        }
    }
}
