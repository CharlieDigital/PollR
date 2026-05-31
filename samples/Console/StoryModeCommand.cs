using System.CommandLine;
using PollR;
using Spectre.Console;

public sealed class StoryModeCommand : Command
{
    public StoryModeCommand()
        : base(
            "story-mode",
            "A guided walkthrough of PollR catch-up and synchronization using a deterministic sequence cursor."
        )
    {
        SetAction(async (_, cancellationToken) => await RunAsync(cancellationToken));
    }

    static async Task RunAsync(CancellationToken cancellationToken)
    {
        var feed = new StoryFeed();
        var PollR = new StoryPollRCaster(feed.ProduceAsync);
        var consumerOne = new StoryConsumer("consumer-1", "deepskyblue1");
        var consumerTwo = new StoryConsumer("consumer-2", "mediumspringgreen");
        var consumerThree = new StoryConsumer("consumer-3", "orchid");
        var consumerFour = new StoryConsumer("consumer-4", "LightPink1");

        WritePanel(
            "PollR story mode",
            "[grey]A producer is writing events to one partition while consumers join at different cursors. Watch PollR replay only the records each late consumer missed, handle a full-history subscriber while a new record is produced, then return everyone to the same live stream.[/]\n\n[grey]Press Enter at each prompt to run exactly one manual tick.[/]"
        );

        WritePanel(
            "Stage 1",
            "[bold]One subscriber starts at cursor 0 and receives new records as the feed advances.[/]\n\n[grey]Because no one else is subscribed yet, each tick reads from consumer-1's cursor and releases one new event.[/]"
        );

        PollR.Subscribe("alpha", 0, consumerOne.Stream, cancellationToken);
        AnsiConsole.MarkupLine(
            "[deepskyblue1]consumer-1[/] subscribes to [grey]alpha[/] at cursor [yellow]0[/]."
        );
        AnsiConsole.WriteLine();

        await RunStepAsync(
            "Tick 1: producer reads after cursor 0; consumer-1 receives event 1.",
            PollR,
            feed,
            [consumerOne],
            cancellationToken
        );
        await RunStepAsync(
            "Tick 2: producer reads after cursor 1; consumer-1 receives event 2.",
            PollR,
            feed,
            [consumerOne],
            cancellationToken
        );
        await RunStepAsync(
            "Tick 3: producer reads after cursor 2; consumer-1 receives event 3.",
            PollR,
            feed,
            [consumerOne],
            cancellationToken
        );

        WritePanel(
            "Stage 2",
            "[bold]A second subscriber joins from the beginning and catches up.[/]\n\n[grey]consumer-1 is already at cursor 3, but consumer-2 is behind, so the next tick reads from cursor 0.[/]"
        );
        PollR.Subscribe("alpha", 0, consumerTwo.Stream, cancellationToken);
        AnsiConsole.MarkupLine(
            "[mediumspringgreen]consumer-2[/] now joins [grey]alpha[/] at cursor [yellow]0[/]."
        );
        AnsiConsole.WriteLine();

        await RunStepAsync(
            "Catch-up tick: producer replays events 1, 2, and 3; only consumer-2 receives them.",
            PollR,
            feed,
            [consumerOne, consumerTwo],
            cancellationToken
        );

        WritePanel(
            "Stage 3",
            "[bold]Both subscribers are synchronized, so each new tick goes to both.[/]"
        );

        await RunStepAsync(
            "Synchronized tick 1: producer reads after cursor 3; consumer-1 and consumer-2 receive event 4.",
            PollR,
            feed,
            [consumerOne, consumerTwo],
            cancellationToken
        );
        await RunStepAsync(
            "Synchronized tick 2: producer reads after cursor 4; consumer-1 and consumer-2 receive event 5.",
            PollR,
            feed,
            [consumerOne, consumerTwo],
            cancellationToken
        );
        await RunStepAsync(
            "Synchronized tick 3: producer reads after cursor 5; consumer-1 and consumer-2 receive event 6.",
            PollR,
            feed,
            [consumerOne, consumerTwo],
            cancellationToken
        );

        WritePanel(
            "Stage 4",
            "[bold]A third subscriber joins with a two-tick lookback.[/]\n\n[grey]consumer-3 starts at cursor 4 while the feed is currently at cursor 6, so it should receive events 5 and 6 before it is caught up.[/]"
        );

        PollR.Subscribe("alpha", 4, consumerThree.Stream, cancellationToken);
        AnsiConsole.MarkupLine(
            "[orchid]consumer-3[/] joins [grey]alpha[/] at cursor [yellow]4[/], while the feed is currently at cursor [yellow]6[/]."
        );
        AnsiConsole.WriteLine();

        await RunStepAsync(
            "Two-tick lookback tick: producer replays events 5 and 6; only consumer-3 receives them.",
            PollR,
            feed,
            [consumerOne, consumerTwo, consumerThree],
            cancellationToken
        );

        await RunStepAsync(
            "Final synchronized tick: all three consumers are at cursor 6, so all three receive event 7.",
            PollR,
            feed,
            [consumerOne, consumerTwo, consumerThree],
            cancellationToken
        );

        WritePanel(
            "Stage 5",
            "[bold]A fourth subscriber joins from cursor 0 while a new record is produced.[/]\n\n[grey]consumer-4 needs the full history, but the feed also releases event 8 during the same tick. PollR should replay events 1 through 8 to consumer-4 while consumers 1, 2, and 3 receive only the new event 8.[/]"
        );

        PollR.Subscribe("alpha", 0, consumerFour.Stream, cancellationToken);
        feed.ReleaseNextRecordWithReplay();

        AnsiConsole.MarkupLine(
            "[LightPink1]consumer-4[/] joins [grey]alpha[/] at cursor [yellow]0[/]."
        );
        AnsiConsole.MarkupLine(
            "[grey]The next tick reads from cursor 0 and the producer releases cursor 8 before returning results.[/]"
        );
        AnsiConsole.WriteLine();

        await RunStepAsync(
            "Full replay plus live tick: consumer-4 receives events 1-8; consumers 1, 2, and 3 receive event 8.",
            PollR,
            feed,
            [consumerOne, consumerTwo, consumerThree, consumerFour],
            cancellationToken
        );

        await PollR.StopAsync();

        await DrainConsumersAsync(
            [consumerOne, consumerTwo, consumerThree, consumerFour],
            cancellationToken
        );

        WritePanel("Story complete", "[green]All consumers are synchronized at the live edge.[/]");
    }

    static async Task RunStepAsync(
        string message,
        StoryPollRCaster PollR,
        StoryFeed feed,
        IReadOnlyList<StoryConsumer> consumers,
        CancellationToken cancellationToken
    )
    {
        PromptForEnter(message);

        var previousProducedCount = feed.ProducedCount;
        await PollR.TickAsync(cancellationToken);

        var produced = feed.ProducedSince(previousProducedCount);

        if (produced.Count == 0)
        {
            AnsiConsole.MarkupLine("[grey]Producer emitted no new records for this cursor.[/]");
        }
        else
        {
            foreach (var result in produced)
            {
                AnsiConsole.MarkupLine(
                    $"[yellow]producer[/] emitted cursor [yellow]{result.Cursor}[/] {Markup.Escape(result.Data.Description)}"
                );
            }
        }

        await DrainConsumersAsync(consumers, cancellationToken);
        AnsiConsole.WriteLine();
    }

    static void PromptForEnter(string message)
    {
        WritePanel("Next tick", $"[bold]{Markup.Escape(message)}[/]");
        AnsiConsole.Markup("[grey]Press Enter to tick...[/]");
        Console.ReadLine();
    }

    static void WritePanel(string title, string markup)
    {
        AnsiConsole.Write(
            new Panel(new Markup(markup)).Header(title).Border(BoxBorder.Rounded).Padding(1, 0)
        );
    }

    static async Task DrainConsumersAsync(
        IReadOnlyList<StoryConsumer> consumers,
        CancellationToken cancellationToken
    )
    {
        foreach (var consumer in consumers)
        {
            await consumer.DrainAsync(cancellationToken);
        }
    }
}

sealed record StoryEvent(long Sequence, string Partition, string Description);

sealed class StoryFeed
{
    readonly List<ProducerResult<StoryEvent, string, long>> _events =
    [
        new(new StoryEvent(1, "alpha", "event 1 for alpha"), 1, "alpha"),
        new(new StoryEvent(2, "alpha", "event 2 for alpha"), 2, "alpha"),
        new(new StoryEvent(3, "alpha", "event 3 for alpha"), 3, "alpha"),
        new(new StoryEvent(4, "alpha", "event 4 for alpha"), 4, "alpha"),
        new(new StoryEvent(5, "alpha", "event 5 for alpha"), 5, "alpha"),
        new(new StoryEvent(6, "alpha", "event 6 for alpha"), 6, "alpha"),
        new(new StoryEvent(7, "alpha", "event 7 for alpha"), 7, "alpha"),
        new(new StoryEvent(8, "alpha", "event 8 for alpha"), 8, "alpha"),
    ];
    readonly List<ProducerResult<StoryEvent, string, long>> _produced = [];
    long _releasedCursor;
    bool _releaseNextRecordWithReplay;

    public int ProducedCount => _produced.Count;

    public void ReleaseNextRecordWithReplay() => _releaseNextRecordWithReplay = true;

    public async IAsyncEnumerable<ProducerResult<StoryEvent, string, long>> ProduceAsync(
        long cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        ProducerResult<StoryEvent, string, long>[] records;

        if (cursor < _releasedCursor)
        {
            if (_releaseNextRecordWithReplay)
            {
                _releasedCursor = Math.Min(_releasedCursor + 1, _events[^1].Cursor);
                _releaseNextRecordWithReplay = false;
            }

            records =
            [
                .. _events.Where(item => item.Cursor > cursor && item.Cursor <= _releasedCursor),
            ];
        }
        else
        {
            _releasedCursor = Math.Min(_releasedCursor + 1, _events[^1].Cursor);
            records = [.. _events.Where(item => item.Cursor == _releasedCursor)];
        }

        await Task.Yield();

        foreach (var record in records)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _produced.Add(record);
            yield return record;
        }
    }

    public IReadOnlyList<ProducerResult<StoryEvent, string, long>> ProducedSince(int index) =>
        _produced.Skip(index).ToArray();
}

sealed class StoryPollRCaster(DataProducer<StoryEvent, string, long> producer)
    : PollRBase<StoryEvent, string, long>(
        producer,
        initialCursorFactory: () => 0,
        clampCursor: cursor => cursor
    );

sealed class StoryConsumer(string name, string color)
{
    public ChannelDataStream<IntervalData<StoryEvent, string, long>> Stream { get; } =
        ChannelDataStream<IntervalData<StoryEvent, string, long>>.CreateUnbounded();

    public async Task DrainAsync(CancellationToken cancellationToken)
    {
        while (Stream.Reader.TryRead(out var item))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (item.TryGetData(out var data))
            {
                AnsiConsole.MarkupLine(
                    $"[{color}]{Markup.Escape(name)}[/] received cursor [yellow]{data.Data.Sequence}[/] {Markup.Escape(data.Data.Description)}"
                );
            }
            else if (item.TryGetStreamCompleted(out _))
            {
                AnsiConsole.MarkupLine($"[{color}]{Markup.Escape(name)} completed[/]");
            }
            else if (item.TryGetError(out var error))
            {
                AnsiConsole.MarkupLine(
                    $"[red]{Markup.Escape(name)} error:[/] {Markup.Escape(error.ErrorMessage)}"
                );
            }

            await Task.Yield();
        }
    }
}
