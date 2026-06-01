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

/// <summary>
/// Base class for implementing a PollR caster. It manages the polling lifecycle, subscriber management, and projection handling.
/// </summary>
/// <typeparam name="TData">The type of the data produced by the producer.</typeparam>
/// <typeparam name="TPartition">The type of the partition key.</typeparam>
/// <typeparam name="TCursor">The type of the cursor used for tracking positions.</typeparam>
/// <param name="producer">The data producer function that generates data based on the cursor position.</param>
/// <param name="initialCursorFactory">A function that provides the initial cursor position when the caster starts.</param>
/// <param name="clampCursor">A function that clamps the cursor position to a valid range, typically used to enforce a maximum lookback window.</param>
/// <param name="pollingInterval">The interval at which to poll for new data. If not provided, a default interval will be used.</param>
/// <param name="cancellationToken">A cancellation token to observe for stopping the caster.</param>
public abstract class PollRBase<TData, TPartition, TCursor>(
    DataProducer<TData, TPartition, TCursor> producer,
    Func<TCursor> initialCursorFactory,
    Func<TCursor, TCursor> clampCursor,
    TimeSpan? pollingInterval = null,
    CancellationToken cancellationToken = default
) : IAsyncDisposable, IDisposable
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    readonly DataProducer<TData, TPartition, TCursor> _producer = producer;
    readonly Lock _lifecycleLock = new();
    readonly SemaphoreSlim _tickLock = new(1, 1);
    readonly System.Collections.Concurrent.ConcurrentDictionary<
        string,
        IProjectionGroup<TData, TPartition, TCursor>
    > _projectionGroups = new(StringComparer.Ordinal);

    readonly PollRContext<TData, TPartition, TCursor> _context =
        new(
            pollingInterval,
            initialCursorFactory,
            clampCursor,
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
        );

    Task? _startTask;
    int _completed;
    int _disposed;

    public Task StartAsync()
    {
        if (Volatile.Read(ref _disposed) == 1)
        {
            return Task.CompletedTask;
        }

        lock (_lifecycleLock)
        {
            _startTask ??= RunCoreLoopAsync();
            return _startTask;
        }
    }

    /// <summary>
    /// This is the core loop that runs the polling process.
    /// </summary>
    /// <remarks>
    /// The loop runs the producer function on each tick and produces the list of
    /// results The results are then broadcast to subscribers via projections.
    ///
    /// The loop is intentionally decoupled from the `TickAsync` method to allow for
    /// external control and easier testing of the tick process as this allows
    /// manual ticking rather than time controlled ticks (harder to test)
    /// </remarks>>
    async Task RunCoreLoopAsync()
    {
        while (!_context.IsCancellationRequested)
        {
            try
            {
                await TickAsync();
                await Task.Delay(_context.PollingInterval, _context.CancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        await CompleteAsync();
    }

    public async Task StopAsync()
    {
        if (Volatile.Read(ref _disposed) == 1)
        {
            return;
        }

        await StopCoreAsync();
    }

    async Task StopCoreAsync()
    {
        await _context.CancelAsync();

        Task? startTask;
        lock (_lifecycleLock)
        {
            startTask = _startTask;
        }

        if (startTask is not null)
        {
            await startTask;
            return;
        }

        await CompleteAsync();
    }

    public void Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        IDataStream<IntervalData<TData, TPartition, TCursor>> stream,
        CancellationToken cancellationToken = default
    )
    {
        if (Volatile.Read(ref _disposed) == 1)
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
    /// A standard polling tick implementation that retrieves data from the
    /// producer and broadcasts it to subscribers and projections.
    /// </summary>
    /// <remarks>
    /// This is meant to allow the tick process to be controllable externally
    /// and easier to test.
    /// </remarks>
    /// <param name="cancellationToken">A cancellation token to observe for stopping the tick process.</param>
    /// <returns>A task that represents the asynchronous tick operation.</returns>
    public async Task TickAsync(CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _disposed) == 1 || _context.IsCancellationRequested)
        {
            return;
        }

        await _tickLock.WaitAsync(cancellationToken);

        try
        {
            if (_context.IsCancellationRequested)
            {
                return;
            }

            using var tickCancellationTokenSource = cancellationToken.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(
                    _context.CancellationToken,
                    cancellationToken
                )
                : null;

            var tickCancellationToken =
                tickCancellationTokenSource?.Token ?? _context.CancellationToken;

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
            await foreach (var item in _producer(cursorPosition, tickCancellationToken))
            {
                if (item.Cursor.CompareTo(latestCursorPosition) > 0)
                {
                    latestCursorPosition = item.Cursor;
                }

                await PartitionResultBroadcastAsync(item, tickCancellationToken);
                await ProjectionResultBroadcastAsync(item, tickCancellationToken);
            }

            _context.CompletePollingTick(latestCursorPosition);
            CompleteProjectionPollingTick(latestCursorPosition);
        }
        catch (OperationCanceledException) when (_context.IsCancellationRequested) { }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            await BroadcastAsync(new ErrorResult(ex.Message), CancellationToken.None);
            await BroadcastProjectionErrorAsync(
                new ErrorResult(ex.Message),
                CancellationToken.None
            );
        }
        finally
        {
            _tickLock.Release();
        }
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await StopCoreAsync();
        _tickLock.Dispose();
        _context.Dispose();
        GC.SuppressFinalize(this);
    }

    private async Task CompleteAsync()
    {
        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            return;
        }

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
        if (Volatile.Read(ref _disposed) == 1 || _context.IsCancellationRequested)
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
