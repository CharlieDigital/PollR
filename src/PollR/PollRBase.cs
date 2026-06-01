namespace PollR;

public delegate IAsyncEnumerable<ProducerResult<TData, TPartition, TCursor>> DataProducer<
    TData,
    TPartition,
    TCursor
>(TCursor cursorPosition, CancellationToken cancellationToken)
    where TCursor : IComparable<TCursor>;

public delegate IAsyncEnumerable<ProducerResult<TData, TPartition>> DataProducer<TData, TPartition>(
    DateTimeOffset cursorPosition,
    CancellationToken cancellationToken
);

public delegate IAsyncEnumerable<ProducerResult<TData, TPartition, TCursor>> PartitionDataProducer<
    TData,
    TPartition,
    TCursor
>(TPartition partition, TCursor cursorPosition, CancellationToken cancellationToken)
    where TPartition : notnull
    where TCursor : IComparable<TCursor>;

public delegate IAsyncEnumerable<ProducerResult<TData, TPartition>> PartitionDataProducer<
    TData,
    TPartition
>(TPartition partition, DateTimeOffset cursorPosition, CancellationToken cancellationToken)
    where TPartition : notnull;

/// <summary>
/// Base class for the existing shared-feed PollR implementation.
/// </summary>
/// <typeparam name="TData">The type of the data produced by the producer.</typeparam>
/// <typeparam name="TPartition">The type of the partition key.</typeparam>
/// <typeparam name="TCursor">The type of the cursor used for tracking positions.</typeparam>
/// <param name="producer">The data producer function that generates data based on the cursor position.</param>
/// <param name="initialCursorFactory">A function that provides the initial cursor position when the caster starts.</param>
/// <param name="clampCursor">A function that clamps the cursor position to a valid range, typically used to enforce a maximum lookback window.</param>
/// <param name="pollingInterval">The interval at which to poll for new data. If not provided, a default interval will be used.</param>
/// <param name="cancellationToken">A cancellation token to observe for stopping the caster.</param>
/// <remarks>
/// The runner lifecycle now lives in <see cref="PollRRunnerBase"/>. This type keeps the
/// original shared-feed cursor and projection behavior while exposing the new shared
/// direct-subscription contract.
/// </remarks>
public abstract class PollRBase<TData, TPartition, TCursor>
    : PollRRunnerBase,
        IPollRSubscriber<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    readonly DataProducer<TData, TPartition, TCursor> _producer;
    readonly System.Collections.Concurrent.ConcurrentDictionary<
        string,
        IProjectionGroup<TData, TPartition, TCursor>
    > _projectionGroups = new(StringComparer.Ordinal);

    readonly PollRContext<TData, TPartition, TCursor> _context;

    protected PollRBase(
        DataProducer<TData, TPartition, TCursor> producer,
        Func<TCursor> initialCursorFactory,
        Func<TCursor, TCursor> clampCursor,
        TimeSpan? pollingInterval = null,
        CancellationToken cancellationToken = default
    )
        : base(pollingInterval, cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(initialCursorFactory);
        ArgumentNullException.ThrowIfNull(clampCursor);

        _producer = producer;
        _context = new(initialCursorFactory, clampCursor);
    }

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
    /// Executes one shared-feed tick: register pending joins, select the next cursor,
    /// stream produced items, then finalize catch-up state.
    /// </summary>
    /// <remarks>
    /// The runner base already serialized this call. This method only owns shared-feed
    /// data flow and projection fan-out.
    /// </remarks>
    protected override async Task ExecuteTickCoreAsync(CancellationToken cancellationToken)
    {
        try
        {
            _context.RegisterPendingSubscribers();
            RegisterPendingProjectionSubscribers();

            var cursorPosition = _context.GetNextCursorPosition();

            foreach (var projectionGroup in _projectionGroups.Values)
            {
                if (
                    projectionGroup.TryGetNextCursorPosition(out var projectionCursorPosition)
                    && projectionCursorPosition.CompareTo(cursorPosition) < 0
                )
                {
                    cursorPosition = projectionCursorPosition;
                }
            }

            var latestCursorPosition = cursorPosition;

            // ⭐️ Here is where the producer function is invoked and the results are
            // processed as an async enumerable stream to reduce memory pressure for
            // manifesting the full collection.
            await foreach (var item in _producer(cursorPosition, cancellationToken))
            {
                if (item.Cursor.CompareTo(latestCursorPosition) > 0)
                {
                    latestCursorPosition = item.Cursor;
                }

                await PartitionResultBroadcastAsync(item, cancellationToken);
                await ProjectionResultBroadcastAsync(item, cancellationToken);
            }

            _context.CompletePollingTick(latestCursorPosition);
            CompleteProjectionPollingTick(latestCursorPosition);
        }
        catch (Exception ex)
        {
            await BroadcastAsync(new ErrorResult(ex.Message), CancellationToken.None);
            await BroadcastProjectionErrorAsync(
                new ErrorResult(ex.Message),
                CancellationToken.None
            );
        }
    }

    /// <summary>
    /// Completes shared-feed direct and projection subscribers exactly once.
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
                new ProjectionGroup<TData, TPartition, TCursor, TPayload>(project)
            )
        )
        {
            throw new InvalidOperationException(
                $"A projection has already been registered with key '{key}'."
            );
        }
    }

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

    private ProjectionGroup<TData, TPartition, TCursor, TPayload> GetProjectionGroup<TPayload>(
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

        if (projectionGroup is ProjectionGroup<TData, TPartition, TCursor, TPayload> typedGroup)
        {
            return typedGroup;
        }

        throw new InvalidOperationException(
            $"Projection '{key}' produces '{projectionGroup.PayloadType}', not '{typeof(TPayload)}'."
        );
    }

    private void RegisterPendingProjectionSubscribers()
    {
        var currentCursorPosition = _context.GetCurrentCursorPosition();

        foreach (var projectionGroup in _projectionGroups.Values)
        {
            projectionGroup.RegisterPendingSubscribers(currentCursorPosition);
        }
    }

    private void CompleteProjectionPollingTick(TCursor cursorPosition)
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            projectionGroup.CompletePollingTick(cursorPosition);
        }
    }

    private async ValueTask ProjectionResultBroadcastAsync(
        ProducerResult<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    )
    {
        // Registered projections are the shared fan-out path. Each projection group owns
        // one projection function for a stable key, so a produced record is projected once
        // per key with matching subscribers instead of once per connected subscriber.
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            await projectionGroup.ProjectAndBroadcastAsync(item, cancellationToken);
        }
    }

    private async ValueTask BroadcastProjectionErrorAsync(
        ErrorResult error,
        CancellationToken cancellationToken
    )
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            await projectionGroup.BroadcastErrorAsync(error, cancellationToken);
        }
    }

    private async ValueTask BroadcastProjectionCompletedAsync(
        StreamCompletedResult completed,
        CancellationToken cancellationToken
    )
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            await projectionGroup.BroadcastCompletedAsync(completed, cancellationToken);
        }
    }

    private void CompleteProjectionSubscribers()
    {
        foreach (var projectionGroup in _projectionGroups.Values)
        {
            projectionGroup.CompleteSubscribers();
        }
    }

    private async Task PartitionResultBroadcastAsync(
        ProducerResult<TData, TPartition, TCursor> item,
        CancellationToken cancellationToken
    )
    {
        if (!_context.TryGetPartitionSubscribers(item.Partition, out var partitionSubscribers))
        {
            return;
        }

        IntervalData<TData, TPartition, TCursor> dataResult = new DataResult<
            TData,
            TPartition,
            TCursor
        >(item.Data, item.Cursor, item.Partition);

        foreach (var subscriber in partitionSubscribers.All)
        {
            if (!subscriber.TryAdvanceCursor(item.Cursor))
            {
                continue;
            }

            await PushAsync(subscriber, dataResult, cancellationToken);
        }
    }

    private async Task BroadcastAsync(
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

    private static async ValueTask PushAsync(
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
}
