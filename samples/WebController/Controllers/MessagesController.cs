using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using PollR;
using PollR.AspNetCore;
using PollR.Samples.WebShared;

namespace WebController.Controllers;

[ApiController]
public sealed class MessagesController(
    PollRCaster<MessageEvent, string> poller,
    IDbContextFactory<MessageDb> dbFactory,
    IHttpContextAccessor httpContextAccessor
) : ControllerBase
{
    // Subscribe with:
    // curl -N http://localhost:5000/events/general
    [HttpGet("/events/{topic}")]
    public IResult Subscribe(string topic) =>
        poller
            // Build an SSE subscription from the current request.
            .ForHttp(httpContextAccessor)
            // Subscribe to the topic partition and fall back to a one minute lookback.
            .WithSubscription(topic, DateTimeOffset.UtcNow.AddMinutes(-1))
            // Emit the topic as the SSE event type.
            .WithSseEventType(partition => partition)
            // Serialize once before fan-out; ASP.NET writes the string as SSE data.
            .WithAdHocSerializedProjection(data => JsonSerializer.Serialize(data.Data));

    // Write with:
    // curl -X POST http://localhost:5000/messages/general -H "Content-Type: application/json" -d '{"text":"hello"}'
    [HttpPost("/messages/{topic}")]
    public async Task<IResult> Write(string topic, WriteMessage request)
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
}
