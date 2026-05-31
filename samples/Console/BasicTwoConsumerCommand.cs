using System.CommandLine;
using PollR;
using Spectre.Console;

public class BasicTwoConsumerCommand : Command
{
    public BasicTwoConsumerCommand()
        : base(
            "basic-two-consumer",
            "A basic example of using PollR with two consumers on different partitions."
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

                var alphaStream = DefaultChannelDataStream<DemoChange, string>.CreateUnbounded();
                var betaStream = DefaultChannelDataStream<DemoChange, string>.CreateUnbounded();

                AnsiConsole.MarkupLine("[bold]Starting basic two-consumer demo[/]");

                PollR.Subscribe(
                    "alpha",
                    DateTimeOffset.UtcNow.AddSeconds(-2),
                    alphaStream,
                    cancellationToken
                );
                PollR.Subscribe(
                    "beta",
                    DateTimeOffset.UtcNow.AddSeconds(-2),
                    betaStream,
                    cancellationToken
                );

                var consumers = new[]
                {
                    DemoConsole.RunConsumerAsync(
                        "alpha-consumer",
                        "deepskyblue1",
                        alphaStream,
                        cancellationToken
                    ),
                    DemoConsole.RunConsumerAsync(
                        "beta-consumer",
                        "mediumspringgreen",
                        betaStream,
                        cancellationToken
                    ),
                };

                await DemoConsole.RunForAsync(PollR, runtime, cancellationToken);
                await Task.WhenAll(consumers);
            }
        );
    }
}
