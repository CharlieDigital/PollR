using System.CommandLine;
using PollR;
using Spectre.Console;

internal sealed record DemoChange(long Sequence, string Partition, string Description);

internal sealed class DemoChangeFeed
{
    readonly Lock _lock = new();
    readonly List<ProducerResult<DemoChange, string>> _changes = [];
    long _sequence;

    public async IAsyncEnumerable<ProducerResult<DemoChange, string>> ProduceAsync(
        DateTimeOffset cursorPosition,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        AddChange("alpha");
        AddChange("beta");
        AddChange("gamma");

        ProducerResult<DemoChange, string>[] changes;

        lock (_lock)
        {
            changes =
            [
                .. _changes
                    .Where(change => change.Timestamp > cursorPosition)
                    .OrderBy(change => change.Timestamp),
            ];
        }

        await Task.Yield();

        foreach (var change in changes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return change;
        }
    }

    void AddChange(string partition)
    {
        var sequence = Interlocked.Increment(ref _sequence);
        var change = new DemoChange(sequence, partition, $"change #{sequence} for {partition}");

        lock (_lock)
        {
            _changes.Add(
                new ProducerResult<DemoChange, string>(change, DateTimeOffset.UtcNow, partition)
            );
        }
    }
}

internal static class DemoConsole
{
    public static Option<int> CreateRuntimeOption()
    {
        var option = new Option<int>("--runtime", "-r")
        {
            Description = "How long to run the sample, in seconds.",
            Required = true,
        };

        option.Validators.Add(result =>
        {
            if (result.GetValueOrDefault<int>() <= 0)
            {
                result.AddError("Runtime must be greater than zero seconds.");
            }
        });

        return option;
    }

    public static async Task RunConsumerAsync(
        string name,
        string color,
        DefaultChannelDataStream<DemoChange, string> stream,
        CancellationToken cancellationToken
    )
    {
        var completed = false;

        try
        {
            await foreach (var item in stream.Reader.ReadAllAsync(cancellationToken))
            {
                if (item.TryGetData(out var data))
                {
                    AnsiConsole.MarkupLine(
                        $"[{color}]{Markup.Escape(name)}[/] [grey]{data.Data.Partition}[/] {Markup.Escape(data.Data.Description)}"
                    );
                }
                else if (item.TryGetError(out var error))
                {
                    AnsiConsole.MarkupLine(
                        $"[red]{Markup.Escape(name)} error:[/] {Markup.Escape(error.ErrorMessage)}"
                    );
                }
                else if (item.TryGetStreamCompleted(out _) && !completed)
                {
                    completed = true;
                    AnsiConsole.MarkupLine($"[{color}]{Markup.Escape(name)} completed[/]");
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
    }

    public static async Task RunForAsync(
        PollRCaster<DemoChange, string> PollR,
        TimeSpan runtime,
        CancellationToken cancellationToken
    )
    {
        var runTask = PollR.StartAsync();

        try
        {
            await Task.Delay(runtime, cancellationToken);
        }
        finally
        {
            await PollR.StopAsync();
            await runTask;
        }
    }
}
