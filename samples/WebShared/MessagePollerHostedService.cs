using Microsoft.Extensions.Hosting;

namespace PollR.Samples.WebShared;

/// <summary>
/// Hosted service that will get DI registered with ASP.NET
/// </summary>
public sealed class MessagePollerHostedService(MessagePoller messagePoller) : IHostedService
{
    Task? _pollerTask;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // StartAsync returns the long-running polling loop task; keep it for shutdown.
        _pollerTask = messagePoller.Poller.StartAsync();
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // StopAsync cancels the loop and completes subscribers before the app exits.
        await messagePoller.Poller.StopAsync();

        if (_pollerTask is not null)
        {
            await _pollerTask.WaitAsync(cancellationToken);
        }

        // Dispose releases the linked cancellation token source held by the poller.
        await messagePoller.Poller.DisposeAsync();
    }
}
