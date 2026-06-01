namespace PollR;

/// <summary>
/// Per-partition polling state for the partitioned poller.
/// </summary>
/// <remarks>
/// This keeps cursor, catch-up, retry, and subscriber state local to one partition.
/// The owning context is responsible for structural concerns such as registry
/// membership, scheduler order, and synchronization.
/// </remarks>
internal sealed class PartitionFeedState<TData, TPartition, TCursor>(TPartition partition)
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    TCursor? _cursorPosition;
    TCursor? _activeCatchUpCursorPosition;
    int _activeProjectionSubscriberCount;
    int _pendingProjectionSubscriberCount;

    /// <summary>
    /// Partition key for this feed state.
    /// </summary>
    public TPartition Partition { get; } = partition;

    /// <summary>
    /// Subscriber registry for this partition.
    /// </summary>
    public PartitionSubscribers<TData, TPartition, TCursor> Subscribers { get; } = new();

    /// <summary>
    /// The next time this partition should be polled.
    /// </summary>
    public DateTimeOffset NextDueAt { get; private set; } = DateTimeOffset.MinValue;

    /// <summary>
    /// True while a poll task is active for this partition.
    /// </summary>
    public bool IsPolling { get; set; }

    /// <summary>
    /// Consecutive partition-local failure count.
    /// </summary>
    public int ConsecutiveFailureCount { get; private set; }

    /// <summary>
    /// True once a current cursor has been established.
    /// </summary>
    public bool HasCursorPosition { get; private set; }

    /// <summary>
    /// True once a partition-local catch-up cursor is active.
    /// </summary>
    public bool HasActiveCatchUpCursorPosition { get; private set; }

    /// <summary>
    /// True when there are no direct subscribers left in either the current or catch-up set.
    /// </summary>
    public bool IsIdle => Subscribers.IsEmpty && _activeProjectionSubscriberCount == 0;

    /// <summary>
    /// True when projection joins are still staged for this partition.
    /// </summary>
    public bool HasPendingProjectionSubscribers => _pendingProjectionSubscriberCount > 0;

    /// <summary>
    /// Registers one subscriber into the partition-local direct subscriber set.
    /// </summary>
    public void AddSubscriber(
        long subscriberId,
        Subscriber<TData, TPartition, TCursor> subscriber,
        Func<TCursor> initialCursorFactory
    )
    {
        var currentCursorPosition = GetCurrentCursorPosition(initialCursorFactory);
        var isCatchingUp = subscriber.CursorPosition.CompareTo(currentCursorPosition) < 0;

        Subscribers.Add(subscriberId, subscriber, isCatchingUp);

        if (
            isCatchingUp
            && (
                !HasActiveCatchUpCursorPosition
                || subscriber.CursorPosition.CompareTo(_activeCatchUpCursorPosition!) < 0
            )
        )
        {
            _activeCatchUpCursorPosition = subscriber.CursorPosition;
            HasActiveCatchUpCursorPosition = true;
        }
    }

    /// <summary>
    /// Returns the partition's current cursor or the configured initial cursor.
    /// </summary>
    public TCursor GetCurrentCursorPosition(Func<TCursor> initialCursorFactory) =>
        HasCursorPosition ? _cursorPosition! : initialCursorFactory();

    /// <summary>
    /// Returns the next cursor to query for this partition.
    /// </summary>
    public TCursor GetNextCursorPosition(
        Func<TCursor> initialCursorFactory,
        Func<TCursor, TCursor> clampCursor
    ) =>
        clampCursor(
            HasActiveCatchUpCursorPosition
                ? _activeCatchUpCursorPosition!
                : GetCurrentCursorPosition(initialCursorFactory)
        );

    /// <summary>
    /// Completes a successful poll and promotes any caught-up subscribers.
    /// </summary>
    public void CompletePollingTick(TCursor latestCursorPosition, DateTimeOffset nextDueAt)
    {
        _cursorPosition = latestCursorPosition;
        HasCursorPosition = true;
        ConsecutiveFailureCount = 0;
        NextDueAt = nextDueAt;

        Subscribers.PromoteCaughtUp(latestCursorPosition);

        _activeCatchUpCursorPosition = default;
        HasActiveCatchUpCursorPosition = false;
    }

    /// <summary>
    /// Records a partition-local retry schedule after a failure.
    /// </summary>
    public void ScheduleRetry(DateTimeOffset nextDueAt)
    {
        ConsecutiveFailureCount++;
        NextDueAt = nextDueAt;
    }

    /// <summary>
    /// Removes one subscriber from the partition-local registry.
    /// </summary>
    public bool TryRemoveSubscriber(
        long subscriberId,
        out Subscriber<TData, TPartition, TCursor>? subscriber
    )
    {
        return Subscribers.TryRemove(subscriberId, out subscriber);
    }

    /// <summary>
    /// Completes all active subscribers for this partition.
    /// </summary>
    public void CompleteSubscribers()
    {
        foreach (var subscriber in Subscribers.All)
        {
            subscriber.Complete(disposeCancellationRegistration: true);
        }
    }

    /// <summary>
    /// Tracks one staged projection subscriber for this partition.
    /// </summary>
    public void AddPendingProjectionSubscriber() => _pendingProjectionSubscriberCount++;

    /// <summary>
    /// Promotes one staged projection subscriber into the active projection set.
    /// </summary>
    public void PromotePendingProjectionSubscriber()
    {
        if (_pendingProjectionSubscriberCount > 0)
        {
            _pendingProjectionSubscriberCount--;
        }

        _activeProjectionSubscriberCount++;
    }

    /// <summary>
    /// Removes one projection subscriber from either the pending or active set.
    /// </summary>
    public void RemoveProjectionSubscriber(bool wasPending)
    {
        if (wasPending)
        {
            if (_pendingProjectionSubscriberCount > 0)
            {
                _pendingProjectionSubscriberCount--;
            }

            return;
        }

        if (_activeProjectionSubscriberCount > 0)
        {
            _activeProjectionSubscriberCount--;
        }
    }
}
