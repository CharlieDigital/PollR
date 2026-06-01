using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Add the common PollR setup (same as WebMinimal, except using PartitionedPollRCaster).
// The partitioned poller treats each active topic as its own polling lane: independent
// cursors, catch-up windows, and failure domains.
builder.Services.AddHttpContextAccessor();

builder.Services.AddDbContextFactory<MessageDb>(options =>
    options.UseInMemoryDatabase("PollR-web-minimal-partitioned")
);

// Register the partitioned poller as a singleton.
// Each active topic polls on its own schedule; idle topics incur zero cost until
// a subscriber joins and activates their lane.
builder.Services.AddSingleton<PartitionedPollRCaster<MessageEvent, string>>(provider =>
    new PartitionedPollRCaster<MessageEvent, string>(
        (topic, cursor, cancellationToken) =>
            ProduceAsync(provider.GetRequiredService<IDbContextFactory<MessageDb>>(), topic, cursor, cancellationToken),
        pollingInterval: TimeSpan.FromSeconds(1),
        lookbackWindow: TimeSpan.FromMinutes(1),
        maxLookbackWindow: TimeSpan.FromMinutes(5),
        maxConcurrentPartitions: 4
    )
    .RegisterSerializedProjection(
        MessageProjection.Full,
        data => JsonSerializer.Serialize(data.Data)
    )
);

builder.Services.AddHostedService<PartitionedPollerHostedService>();

var app = builder.Build();

// Subscribe with:
// curl -N http://localhost:5057/events/general
app.MapGet(
    "/events/{topic}",
    (
        string topic,
        PartitionedPollRCaster<MessageEvent, string> partitionedPoller,
        IHttpContextAccessor httpContextAccessor
    ) =>
        partitionedPoller
            // Same ForHttp and subscription builder as WebMinimal, just on partitioned poller.
            .ForHttp(httpContextAccessor)
            // WithSubscription activates the topic lane if dormant.
            .WithSubscription(topic, DateTimeOffset.UtcNow.AddMinutes(-1))
            // Each topic is emitted as its own SSE event name.
            .WithSseEventType(partition => partition)
            // Registered projection runs once per record and is shared across subscribers.
            .WithRegisteredSerializedProjection(MessageProjection.Full)
);

// Subscribe to the ad-hoc projection with:
// curl -N http://localhost:5057/events/general/text
app.MapGet(
    "/events/{topic}/text",
    (
        string topic,
        PartitionedPollRCaster<MessageEvent, string> partitionedPoller,
        IHttpContextAccessor httpContextAccessor
    ) =>
        partitionedPoller
            .ForHttp(httpContextAccessor)
            .WithSubscription(topic, DateTimeOffset.UtcNow.AddMinutes(-1))
            .WithSseEventType(partition => $"{partition}-text")
            // Ad-hoc projections run per subscriber; more flexible, higher cost than registered.
            .WithAdHocSerializedProjection(data => JsonSerializer.Serialize(new { data.Data.Text }))
);

// Write with:
// curl -X POST http://localhost:5057/messages/general -H "Content-Type: application/json" -d '{"text":"hello"}'
app.MapPost(
    "/messages/{topic}",
    async (string topic, WriteMessage request, IDbContextFactory<MessageDb> dbFactory) =>
    {
        await using var db = await dbFactory.CreateDbContextAsync();
        var message = new Message
        {
            Topic = topic,
            Text = request.Text,
            CreatedAt = DateTimeOffset.UtcNow,
        };

        db.Messages.Add(message);
        await db.SaveChangesAsync();

        return Results.Created($"/messages/{topic}/{message.Id}", message);
    }
);

app.Run();

// ProduceAsync is called once per active topic. Filter by topic AND cursor in the database
// query so PollR never loads rows for other topics.
static async IAsyncEnumerable<ProducerResult<MessageEvent, string>> ProduceAsync(
    IDbContextFactory<MessageDb> dbFactory,
    string topic,
    DateTimeOffset cursor,
    [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
)
{
    await using var db = await dbFactory.CreateDbContextAsync(cancellationToken);

    await foreach (
        // The polling loop is a single function that performs a DB query using the
        // topic and cursor parameters. PollR handles the scheduling, partitioning,
        // and cursor management; the producer just returns results for the requested
        // topic and cursor.  This is the "core" retrieval of records to feed the poller.
        var message in db
            .Messages.AsNoTracking()
            .Where(m => m.Topic == topic && m.CreatedAt > cursor)
            .OrderBy(m => m.CreatedAt)
            .AsAsyncEnumerable()
            .WithCancellation(cancellationToken)
    )
    {
        // Yield each result to prevent manifesting in memory.
        yield return new ProducerResult<MessageEvent, string>(
            new MessageEvent(message.Id, message.Topic, message.Text, message.CreatedAt),
            message.CreatedAt,
            message.Topic
        );
    }
}

// Hosted service that manages the poller's start/stop lifecycle.
// StartAsync fires the long-running polling loop. StopAsync signals a clean
// shutdown, drains in-flight ticks, and completes all open SSE subscribers so
// browsers receive a proper close event rather than an abrupt disconnect.
sealed class PartitionedPollerHostedService(
    PartitionedPollRCaster<MessageEvent, string> poller
) : IHostedService
{
    Task? _pollerTask;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // StartAsync returns the long-running loop task; keep it to await during shutdown.
        _pollerTask = poller.StartAsync();
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Signal the polling loop to stop and complete all active subscribers
        // before the host tears down request processing.
        await poller.StopAsync();

        if (_pollerTask is not null)
        {
            await _pollerTask.WaitAsync(cancellationToken);
        }

        // Release the CancellationTokenSource and any other resources the poller holds.
        await poller.DisposeAsync();
    }
}
