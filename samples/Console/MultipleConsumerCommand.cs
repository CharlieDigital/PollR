using System.CommandLine;
using PollR;
using Spectre.Console;

public class MultipleConsumerCommand : Command
{
    public MultipleConsumerCommand()
        : base(
            "multiple-consumer",
            "A basic example of using PollR with multiple consumers on different partitions; consumers have random lifecycles and will come and go, simulating a server accepting connections."
        )
    {
        var runtimeOption = DemoConsole.CreateRuntimeOption();
        Options.Add(runtimeOption);

        SetAction(
            async (parseResult, cancellationToken) =>
            {
                var runtime = TimeSpan.FromSeconds(parseResult.GetRequiredValue(runtimeOption));
                var feed = new DemoChangeFeed();

                var PollR = new PollRCaster<DemoChange, string>(
                    feed.ProduceAsync,
                    pollingInterval: TimeSpan.FromMilliseconds(500),
                    lookbackWindow: TimeSpan.FromSeconds(2),
                    maxLookbackWindow: TimeSpan.FromSeconds(10),
                    cancellationToken: cancellationToken
                );

                var consumerTasks = new List<Task>();

                using var runtimeCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken
                );

                AnsiConsole.MarkupLine("[bold]Starting multiple-consumer demo[/]");

                var runTask = PollR.StartAsync();
                var lifecycleTask = RunConsumerLifecyclesAsync(
                    PollR,
                    consumerTasks,
                    runtimeCancellation.Token
                );

                try
                {
                    await Task.Delay(runtime, cancellationToken);
                }
                finally
                {
                    await runtimeCancellation.CancelAsync();
                    await lifecycleTask;
                    await PollR.StopAsync();
                    await runTask;
                    await Task.WhenAll(consumerTasks);
                }
            }
        );
    }

    static async Task RunConsumerLifecyclesAsync(
        PollRCaster<DemoChange, string> PollR,
        List<Task> consumerTasks,
        CancellationToken cancellationToken
    )
    {
        var partitions = new[] { "alpha", "beta", "gamma" };
        var colors = new[]
        {
            "deepskyblue1",
            "mediumspringgreen",
            "mediumpurple1",
            "orange1",
            "hotpink",
        };
        var consumerNumber = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var partition = partitions[Random.Shared.Next(partitions.Length)];
            var color = colors[consumerNumber % colors.Length];
            var name = $"consumer-{++consumerNumber}";
            var stream = DefaultChannelDataStream<DemoChange, string>.CreateUnbounded();
            var consumerCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken
            );

            PollR.Subscribe(
                partition,
                DateTimeOffset.UtcNow.AddSeconds(-Random.Shared.Next(1, 6)),
                stream,
                consumerCancellation.Token
            );
            AnsiConsole.MarkupLine(
                $"[grey]joined[/] [{color}]{Markup.Escape(name)}[/] [grey]on {partition}[/]"
            );

            consumerTasks.Add(
                DemoConsole.RunConsumerAsync(name, color, stream, consumerCancellation.Token)
            );

            _ = EndConsumerAfterDelayAsync(name, color, consumerCancellation, cancellationToken);

            try
            {
                await Task.Delay(
                    TimeSpan.FromMilliseconds(Random.Shared.Next(700, 1600)),
                    cancellationToken
                );
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    static async Task EndConsumerAfterDelayAsync(
        string name,
        string color,
        CancellationTokenSource consumerCancellation,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(Random.Shared.Next(2, 6)), cancellationToken);
            AnsiConsole.MarkupLine($"[grey]leaving[/] [{color}]{Markup.Escape(name)}[/]");
            await consumerCancellation.CancelAsync();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await consumerCancellation.CancelAsync();
        }
        finally
        {
            consumerCancellation.Dispose();
        }
    }
}
