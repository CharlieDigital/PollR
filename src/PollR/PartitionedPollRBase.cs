namespace PollR;

/// <summary>
/// Generic base for the partitioned poller family.
/// </summary>
/// <remarks>
/// This separates partition scheduling from the existing shared-feed poller. Each
/// active partition owns its own cursor, catch-up lane, retry state, and failure
/// domain while the runner base still owns manual ticking and lifecycle.
/// </remarks>
public abstract class PartitionedPollRBase<TData, TPartition, TCursor>
    : PollRRunnerBase,
        IPollRSubscriber<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    readonly PartitionDataProducer<TData, TPartition, TCursor> _producer;
    readonly PartitionedPollRContext<TData, TPartition, TCursor> _context;
    readonly System.Collections.Concurrent.ConcurrentDictionary<
        string,
        IPartitionProjectionGroup<TData, TPartition, TCursor>
    > _projectionGroups = new(StringComparer.Ordinal);

    protected PartitionedPollRBase(
        PartitionDataProducer<TData, TPartition, TCursor> producer,
        Func<TCursor> initialCursorFactory,
        Func<TCursor, TCursor> clampCursor,
        TimeSpan? pollingInterval = null,
        int? maxConcurrentPartitions = null,
        CancellationToken cancellationToken = default
    )
        : base(pollingInterval, cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(initialCursorFactory);
        ArgumentNullException.ThrowIfNull(clampCursor);

        _producer = producer;
        _context = new(PollingInterval, initialCursorFactory, clampCursor);
        // Callout: the context stays responsible for active partition lifetime. Projection
        // groups only report whether a partition still has projection activity.
        MaxConcurrentPartitions =
            maxConcurrentPartitions ?? Math.Max(Environment.ProcessorCount * 2, 8);
    }

    /// <summary>
    /// Maximum number of partitions that may poll concurrently.
    /// </summary>
    protected int MaxConcurrentPartitions { get; }

    public void Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        IDataStream<IntervalData<TData, TPartition, TCursor>> stream,
        CancellationToken cancellationToken = default
    )
    {
        if (IsDisposed || IsRunnerCancellationRequested)
        {
            stream.Complete(cancellationToken);
            return;
        }

        _context.Subscribe(partition, cursorPosition, stream, cancellationToken);
    }

    public ChannelDataStream<IntervalData<TData, TPartition, TCursor>> Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        CancellationToken cancellationToken = default
    ) =>
        Subscribe(
            partition,
            cursorPosition,
            ChannelDataStream<IntervalData<TData, TPartition, TCursor>>.DefaultBoundedCapacity,
            cancellationToken
        );

    public ChannelDataStream<IntervalData<TData, TPartition, TCursor>> Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    )
    {
        var stream = ChannelDataStream<
            IntervalData<TData, TPartition, TCursor>
        >.CreateBoundedDropWrite(streamCapacity);
        Subscribe(partition, cursorPosition, stream, cancellationToken);
        return stream;
    }

    /// <summary>
    /// Runs one partitioned tick: drain staged joins, snapshot due partitions, then poll them.
    /// </summary>
    protected override async Task ExecuteTickCoreAsync(CancellationToken cancellationToken)
    {
        _context.RegisterPendingSubscribers();
        RegisterPendingProjectionSubscribers();

        var utcNow = DateTimeOffset.UtcNow;
        var duePartitions = _context.GetDuePartitions(utcNow);

        try
        {
            if (duePartitions.Count == 0)
            {
                return;
            }

            if (duePartitions.Count <= MaxConcurrentPartitions)
            {
                await Task.WhenAll(
                    duePartitions.Select(partitionState =>
                        PollPartitionAsync(partitionState, utcNow, cancellationToken)
                    )
                );
                return;
            }

            using var concurrencyGate = new SemaphoreSlim(
                MaxConcurrentPartitions,
                MaxConcurrentPartitions
            );

            await Task.WhenAll(
                duePartitions.Select(partitionState =>
                    PollPartitionWithConcurrencyAsync(
                        partitionState,
                        utcNow,
                        concurrencyGate,
                        cancellationToken
                    )
                )
            );
        }
        finally
        {
            _context.AdvanceScanStartIndex();
        }
    }

    /// <summary>
    /// Delays the background loop until the next partition becomes due.
    /// </summary>
    protected override ValueTask<TimeSpan> GetDelayAfterTickAsync(
        CancellationToken cancellationToken
    ) => ValueTask.FromResult(_context.GetDelayUntilNextDue(DateTimeOffset.UtcNow));

    /// <summary>
    /// Completes all active direct subscribers for the partitioned poller.
    /// </summary>
    protected override async Task CompleteCoreAsync()
    {
        await BroadcastAsync(new StreamCompletedResult(), CancellationToken.None);
        await BroadcastProjectionCompletedAsync(
            new StreamCompletedResult(),
            CancellationToken.None
        );
        _context.CompleteSubscribers();
        CompleteProjectionSubscribers();
    }

    /// <summary>
    /// Registers one shared projection key for the partitioned poller.
    /// </summary>
    protected void RegisterProjectionCore<TPayload>(
        string key,
        Func<DataResult<TData, TPartition, TCursor>, TPayload> project
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(project);

        if (
            !_projectionGroups.TryAdd(
                key,
                new PartitionProjectionGroup<TData, TPartition, TCursor, TPayload>(
                    project,
                    _context
                )
            )
        )
        {
            throw new InvalidOperationException(
                $"A projection has already been registered with key '{key}'."
            );
        }
    }

    /// <summary>
    /// Subscribes to one registered partition-aware projection.
    /// </summary>
    protected ChannelDataStream<
        IntervalData<TPayload, TPartition, TCursor>
    > SubscribeProjectionCore<TPayload>(
        string key,
        TPartition partition,
        TCursor cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    )
    {
        if (IsDisposed || IsRunnerCancellationRequested)
        {
            var completedStream = ChannelDataStream<
                IntervalData<TPayload, TPartition, TCursor>
            >.CreateBoundedDropWrite(streamCapacity);
            completedStream.Complete(cancellationToken);
            return completedStream;
        }

        var projectionGroup = GetProjectionGroup<TPayload>(key);
        return projectionGroup.Subscribe(
            partition,
            _context.ClampCursorPosition(cursorPosition),
            streamCapacity,
            cancellationToken
        );
    }

    async Task PollPartitionWithConcurrencyAsync(
        PartitionFeedState<TData, TPartition, TCursor> partitionState,
        DateTimeOffset utcNow,
        SemaphoreSlim concurrencyGate,
        CancellationToken cancellationToken
    )
    {
        await concurrencyGate.WaitAsync(cancellationToken);

        try
        {
            await PollPartitionAsync(partitionState, utcNow, cancellationToken);
        }
        finally
        {
            concurrencyGate.Release();
        }
    }

    /// <summary>
    /// Polls one partition and keeps errors local to that partition's subscribers.
    /// </summary>
    async Task PollPartitionAsync(
        PartitionFeedState<TData, TPartition, TCursor> partitionState,
        DateTimeOffset utcNow,
        CancellationToken cancellationToken
    )
    {
        try
        {
            var cursorPosition = _context.GetNextCursorPosition(partitionState);

            foreach (var projectionGroup in _projectionGroups.Values)
            {
                if (
                    projectionGroup.TryGetNextCursorPosition(
                        partitionState.Partition,
                        out var projectionCursorPosition
                    )
                    && projectionCursorPosition.CompareTo(cursorPosition) < 0
                )
                {
                    cursorPosition = projectionCursorPosition;
                }
            }

            var latestCursorPosition = cursorPosition;

            // Records stream through immediately; the poller does not buffer a full partition result set.
            await foreach (
                var item in _producer(partitionState.Partition, cursorPosition, cancellationToken)
            )
            {
                if (item.Cursor.CompareTo(latestCursorPosition) > 0)
                {
                    latestCursorPosition = item.Cursor;
                }

                // Fan out the raw record and projection results concurrently for the same item.
                await Task.WhenAll(
                    PartitionResultBroadcastAsync(partitionState, item, cancellationToken),
                    ProjectionResultBroadcastAsync(item, cancellationToken).AsTask()
                );
            }

            partitionState.CompletePollingTick(latestCursorPosition, utcNow + PollingInterval);
            CompleteProjectionPollingTick(partitionState.Partition, latestCursorPosition);
        }
        catch (Exception ex)
        {
            var retryDelay = TimeSpan.FromTicks(
                Math.Min(
                    PollingInterval.Ticks * Math.Max(1, partitionState.ConsecutiveFailureCount + 1),
                    PollingInterval.Ticks * 5
                )
            );

            partitionState.ScheduleRetry(utcNow + retryDelay);

            await BroadcastPartitionAsync(
                partitionState,
                new ErrorResult(ex.Message),
                CancellationToken.None
            );
            await BroadcastProjectionErrorAsync(
                partitionState.Partition,
                new ErrorResult(ex.Message),
                CancellationToken.None
            );
        }
        finally
        {
            _context.CompletePartitionPoll(partitionState.Partition);
        }
    }

    /// <summary>
    /// Broadcasts one produced record to matching direct subscribers on the partition only.
    /// </summary>
    async Task PartitionResultBroadcastAsync(
        PartitionFeedState<TData, TPartition, TCursor> partitionState,
        ProducerResult<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    )
    {
        IntervalData<TData, TPartition, TCursor> dataResult = new DataResult<
            TData,
            TPartition,
            TCursor
        >(item.Data, item.Cursor, item.Partition);

        foreach (var subscriber in partitionState.Subscribers.All)
        {
            if (!subscriber.TryAdvanceCursor(item.Cursor))
            {
                continue;
            }

            await PushAsync(subscriber, dataResult, cancellationToken);
        }
    }

    /// <summary>
    /// Broadcasts one terminal item to every active direct subscriber.
    /// </summary>
    async Task BroadcastAsync(
        IntervalData<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    )
    {
        foreach (var subscriber in _context.Subscribers)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await PushAsync(subscriber, item, cancellationToken);
        }
    }

    /// <summary>
    /// Broadcasts one terminal item to a single partition only.
    /// </summary>
    async Task BroadcastPartitionAsync(
        PartitionFeedState<TData, TPartition, TCursor> partitionState,
        IntervalData<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    )
    {
        foreach (var subscriber in partitionState.Subscribers.All)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await PushAsync(subscriber, item, cancellationToken);
        }
    }

    static async ValueTask PushAsync(
        Subscriber<TData, TPartition, TCursor> subscriber,
        IntervalData<TData, TPartition, TCursor> item,
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

    PartitionProjectionGroup<TData, TPartition, TCursor, TPayload> GetProjectionGroup<TPayload>(
        string key
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (!_projectionGroups.TryGetValue(key, out var projectionGroup))
        {
            throw new InvalidOperationException(
                $"No projection has been registered with key '{key}'."
            );
        }

        if (
            projectionGroup
            is PartitionProjectionGroup<TData, TPartition, TCursor, TPayload> typedGroup
        )
        {
            return typedGroup;
        }

        throw new InvalidOperationException(
            $"Projection '{key}' produces '{projectionGroup.PayloadType}', not '{typeof(TPayload)}'."
        );
    }

    void RegisterPendingProjectionSubscribers()
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            projectionGroup.RegisterPendingSubscribers();
        }
    }

    void CompleteProjectionPollingTick(TPartition partition, TCursor cursorPosition)
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            projectionGroup.CompletePollingTick(partition, cursorPosition);
        }
    }

    async ValueTask ProjectionResultBroadcastAsync(
        ProducerResult<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    )
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            await projectionGroup.ProjectAndBroadcastAsync(item, cancellationToken);
        }
    }

    async ValueTask BroadcastProjectionErrorAsync(
        TPartition partition,
        ErrorResult error,
        CancellationToken cancellationToken
    )
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            await projectionGroup.BroadcastErrorAsync(partition, error, cancellationToken);
        }
    }

    async ValueTask BroadcastProjectionCompletedAsync(
        StreamCompletedResult completed,
        CancellationToken cancellationToken
    )
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            await projectionGroup.BroadcastCompletedAsync(completed, cancellationToken);
        }
    }

    void CompleteProjectionSubscribers()
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            projectionGroup.CompleteSubscribers();
        }
    }
}
