namespace PollR;

/// <summary>
/// Thin lifecycle base shared by PollR runner implementations.
/// </summary>
/// <remarks>
/// This owns only runner mechanics: linked cancellation, background loop,
/// one-at-a-time ticks, stop/dispose idempotence, and final completion.
/// Derived types keep scheduler, cursor, and subscriber state local.
/// </remarks>
public abstract class PollRRunnerBase(
    TimeSpan? pollingInterval = null,
    CancellationToken cancellationToken = default
) : IPollRRunner, IAsyncDisposable, IDisposable
{
    static readonly TimeSpan DefaultPollingInterval = TimeSpan.FromMilliseconds(100);

    readonly Lock _lifecycleLock = new();
    readonly SemaphoreSlim _tickGate = new(1, 1);
    readonly CancellationTokenSource _runnerCancellationTokenSource =
        CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

    Task? _startTask;
    int _completed;
    int _disposed;

    /// <summary>
    /// Shared polling cadence used by the background loop when a derived runner does not
    /// need a more specific delay.
    /// </summary>
    protected TimeSpan PollingInterval { get; } = pollingInterval ?? DefaultPollingInterval;

    /// <summary>
    /// Cancellation token for the lifetime of the runner.
    /// </summary>
    protected CancellationToken RunnerCancellationToken => _runnerCancellationTokenSource.Token;

    /// <summary>
    /// Indicates that the runner lifetime has been canceled.
    /// </summary>
    protected bool IsRunnerCancellationRequested =>
        _runnerCancellationTokenSource.IsCancellationRequested;

    /// <summary>
    /// Indicates that disposal has started.
    /// </summary>
    protected bool IsDisposed => Volatile.Read(ref _disposed) == 1;

    public Task StartAsync()
    {
        if (IsDisposed)
        {
            return Task.CompletedTask;
        }

        lock (_lifecycleLock)
        {
            _startTask ??= RunLoopAsync();
            return _startTask;
        }
    }

    public async Task StopAsync()
    {
        if (IsDisposed)
        {
            return;
        }

        await StopCoreAsync();
    }

    /// <summary>
    /// Serializes manual ticks and links caller cancellation to runner cancellation.
    /// </summary>
    public async Task TickAsync(CancellationToken cancellationToken = default)
    {
        if (IsDisposed || IsRunnerCancellationRequested)
        {
            return;
        }

        await _tickGate.WaitAsync(cancellationToken);

        try
        {
            if (IsRunnerCancellationRequested)
            {
                return;
            }

            using var tickCancellationTokenSource = cancellationToken.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(
                    RunnerCancellationToken,
                    cancellationToken
                )
                : null;

            var tickCancellationToken =
                tickCancellationTokenSource?.Token ?? RunnerCancellationToken;

            await ExecuteTickCoreAsync(tickCancellationToken);
        }
        catch (OperationCanceledException) when (IsRunnerCancellationRequested) { }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        finally
        {
            _tickGate.Release();
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
        DisposeCore();
        _tickGate.Dispose();
        _runnerCancellationTokenSource.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Background run loop delegates tick behavior to the derived runner and only owns
    /// the outer lifetime orchestration.
    /// </summary>
    async Task RunLoopAsync()
    {
        while (!IsRunnerCancellationRequested)
        {
            try
            {
                await TickAsync();

                var delay = await GetDelayAfterTickAsync(RunnerCancellationToken);

                // Callout: zero or negative delays are allowed for derived runners that
                // want the next iteration to proceed immediately.
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, RunnerCancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        await CompleteOnceAsync();
    }

    async Task StopCoreAsync()
    {
        await _runnerCancellationTokenSource.CancelAsync();

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

        await CompleteOnceAsync();
    }

    async Task CompleteOnceAsync()
    {
        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            return;
        }

        await CompleteCoreAsync();
    }

    /// <summary>
    /// Derived runners can override the background delay without changing the shared loop.
    /// </summary>
    protected virtual ValueTask<TimeSpan> GetDelayAfterTickAsync(
        CancellationToken cancellationToken
    ) => ValueTask.FromResult(PollingInterval);

    protected virtual void DisposeCore() { }

    protected abstract Task ExecuteTickCoreAsync(CancellationToken cancellationToken);

    protected abstract Task CompleteCoreAsync();
}
