using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using PollR;
using PollR.AspNetCore;
using PollR.Samples.WebShared;

var builder = WebApplication.CreateBuilder(args);

// Add the common PollR setup from the shared library (optional; tweak for your project)
builder.Services.AddPollR("PollR-web-minimal");

var app = builder.Build();

// Subscribe with:
// curl -N http://localhost:5000/events/general
app.MapGet(
    "/events/{topic}",
    (
        string topic,
        PollRCaster<MessageEvent, string> poller,
        IHttpContextAccessor httpContextAccessor
    ) =>
        poller
            // Build an SSE subscription from the current request.
            .ForHttp(httpContextAccessor)
            // Subscribe to the topic partition and fall back to a one minute lookback.
            .WithSubscription(topic, DateTimeOffset.UtcNow.AddMinutes(-1))
            // Emit the topic as the SSE event type.
            .WithSseEventType(partition => partition)
            // Use a registered projection so endpoints can share a stable serialization contract (higher perf)
            .WithRegisteredSerializedProjection(MessageProjection.Full)
);

// Subscribe to the ad-hoc projection with:
// curl -N http://localhost:5000/events/general/text
app.MapGet(
    "/events/{topic}/text",
    (
        string topic,
        PollRCaster<MessageEvent, string> poller,
        IHttpContextAccessor httpContextAccessor
    ) =>
        poller
            // Build a separate subscription to the same topic partition.
            .ForHttp(httpContextAccessor)
            .WithSubscription(topic, DateTimeOffset.UtcNow.AddMinutes(-1))
            .WithSseEventType(partition => $"{partition}-text")
            // Ad-hoc projections can capture endpoint-specific shape and run per subscriber (more flexible)
            .WithAdHocSerializedProjection(data => JsonSerializer.Serialize(new { data.Data.Text }))
);

// Write with:
// curl -X POST http://localhost:5000/messages/general -H "Content-Type: application/json" -d '{"text":"hello"}'
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
