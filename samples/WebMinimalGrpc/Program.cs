using System.Text.Json;
using System.Text.Json.Serialization;
using Grpc.Core;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;
using PollR;
using PollR.Samples.WebMinimalGrpc;
using PollR.Samples.WebShared;

var builder = WebApplication.CreateBuilder(args);

// This demo intentionally uses cleartext HTTP/2 so grpcurl can connect with -plaintext.
// Because there is no TLS/ALPN, the port is HTTP/2-only and curl uses --http2-prior-knowledge.
builder.WebHost.ConfigureKestrel(options =>
    options.ListenLocalhost(5056, listenOptions => listenOptions.Protocols = HttpProtocols.Http2)
);

builder.Services.AddGrpc();

// The sample uses EF Core InMemory as the source of truth for the stream.
builder.Services.AddDbContextFactory<MessageDb>(options =>
    options.UseInMemoryDatabase("PollR-web-minimal-grpc")
);

// One singleton poller is shared by all gRPC subscribers on this node.
builder.Services.AddSingleton<MessagePoller>();
builder.Services.AddSingleton<PollRCaster<MessageEvent, string>>(provider =>
    provider
        .GetRequiredService<MessagePoller>()
        // Source-generated JSON avoids reflection and keeps the shared serializer fast.
        .RegisterSerializedProjection(data =>
            JsonSerializer.Serialize(data.Data, MessageJsonContext.Default.MessageEvent)
        )
);

// The hosted service starts polling and stops/disposes the poller with the host.
builder.Services.AddHostedService<MessagePollerHostedService>();

var app = builder.Build();

// Subscribe with:
// grpcurl -plaintext -import-path samples/WebMinimalGrpc/Protos -proto messages.proto -d '{"topic":"general"}' localhost:5056 pollr.samples.Messages/Subscribe
app.MapGrpcService<MessagesGrpcService>();

// Write with:
// curl --http2-prior-knowledge -X POST http://localhost:5056/messages/general -H "Content-Type: application/json" -d '{"text":"hello"}'
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

sealed class MessagesGrpcService(PollRCaster<MessageEvent, string> poller) : Messages.MessagesBase
{
    public override async Task Subscribe(
        SubscribeRequest request,
        IServerStreamWriter<MessageEnvelope> responseStream,
        ServerCallContext context
    )
    {
        var cursor = DateTimeOffset.TryParse(request.Cursor, out var requestedCursor)
            ? requestedCursor
            : DateTimeOffset.UtcNow.AddMinutes(-1);

        // Subscribe to the registered serialized projection. The stream carries the
        // precomputed JSON string, so gRPC subscribers do not each pay serialization cost.
        var stream = poller.SubscribeSerializedProjection(
            MessageProjection.Full,
            request.Topic,
            cursor,
            context.CancellationToken
        );

        try
        {
            await foreach (var item in stream.Reader.ReadAllAsync(context.CancellationToken))
            {
                if (item.TryGetData(out var data))
                {
                    await responseStream.WriteAsync(
                        new MessageEnvelope
                        {
                            Id = data.Cursor.ToString("O"),
                            Topic = data.Partition,
                            Data = data.Data,
                        },
                        context.CancellationToken
                    );
                }
                else if (item.TryGetError(out var error))
                {
                    throw new RpcException(new Status(StatusCode.Internal, error.ErrorMessage));
                }
            }
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // Client disconnects cancel the gRPC call; that is normal for streaming consumers.
        }
    }
}

[JsonSerializable(typeof(MessageEvent))]
partial class MessageJsonContext : JsonSerializerContext;
