# PollR

PollR is a .NET 10, C#14 library for building long-running, streaming pollers.

Add the ASP.NET adapter for easy Server-Sent Events (SSE) integration or build your own custom adapter for other scenarios.

Provides a no-infrastructure, easy to understand way to implement streaming data polls over SSE.

![PollR Polar](./media/pollr-dark.png)

## Usage Overview

```shell
dotnet add package PollR.AspNetCore
```

A bare minimum setup using `PollRCaster` (the global feed poller) exposes a cursor-backed data source as Server-Sent Events:

```csharp
using System.Text.Json;
using PollR;
using PollR.AspNetCore;

enum MessageProjection
{
    Full
}

record MessageEvent(long Id, string Tenant, string Text, DateTimeOffset CreatedAt);

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpContextAccessor();

var app = builder.Build();

var poller = new PollRCaster<MessageEvent, string>(
    async (cursor, cancellationToken) =>
    {
        // PollR asks for records newer than the cursor it needs.
        // Assume `ReadMessagesAsync` reads from a global Pg table.
        await foreach (var message in ReadMessagesAsync(cursor, cancellationToken))
        {
            // Each record carries data, cursor, and partition.
            yield return new ProducerResult<MessageEvent, string>(
                message,
                message.CreatedAt,
                message.Tenant
            );
        }
    }
)
.RegisterSerializedProjection(
    MessageProjection.Full,
    data => JsonSerializer.Serialize(data.Data)
);
// 👆 Register the shared serialized shape *once* at startup.
// This is the low-allocation path: for each record, JSON serialization runs once
// per projection key and the resulting string is shared by matching subscribers.

app.MapGet(
    "/events/{tenant}",
    (string tenant, IHttpContextAccessor httpContextAccessor) =>
        poller
            // Read Last-Event-ID and subscribe this request to one partition.
            .ForHttp(httpContextAccessor)
            .WithSubscription(tenant, DateTimeOffset.UtcNow.AddMinutes(-1))
            // Send the tenant as the SSE event name.
            .WithSseEventType(partition => partition)
            // Use the registered serialized payload; no per-client JSON work.
            .WithRegisteredSerializedProjection(MessageProjection.Full)
);

// Start polling when the server starts and stop cleanly with the app.
app.Lifetime.ApplicationStarted.Register(() => _ = poller.StartAsync());
app.Lifetime.ApplicationStopping.Register(() => _ = poller.StopAsync());

app.Run();
```

Then in the browser, consume the stream with `EventSource`:

```js
const tenant = "tenant-1";

// EventSource keeps the HTTP connection open and reconnects automatically.
const events = new EventSource(`/events/${encodeURIComponent(tenant)}`);

// This matches WithSseEventType(partition => partition) on the server.
events.addEventListener(tenant, event => {
  // event.lastEventId is the cursor the browser sends back as Last-Event-ID
  // if the connection drops and reconnects.
  const message = JSON.parse(event.data);
  console.log("message", event.lastEventId, message);
});

// Close the stream when the page no longer needs live updates.
// events.close();
```

### Terminal Demo

The terminal demo shows 5 clients connecting on 3 partitions consuming the same stream and allows interactively sending messages to the in-memory EF database.

https://github.com/user-attachments/assets/46b59813-279f-448f-8489-19c57e1f28db

### Story Mode Demo

The story mode demo walks through the use case with a user controlled "tick" on `ENTER` that demonstrates what PollR is doing interactively.

https://github.com/user-attachments/assets/7878ac47-ab02-4b26-be6f-5d268066a6b9

## Motivation

In many applications, you want to efficiently read once and broadcast the data to many client consumers.

This can be done with web sockets, for example, but typically requires additional infrastructure to set up.

It also does not handle the "catch-up" scenario very well when a new client joins the same stream.

In other cases, when clients first connect to the server, they may perform a bootstrap load that is expensive to perform multiple times in a tight window (multiple users request the same bootstrap data in a small slice of time).

Ideally, you load the data once and then broadcast it to all of the clients that are interested in the data and then as the consumers stabilize, you can purely read forward.

Ring buffers are another alternative, but require storing the data in memory and does not support arbitrary catchup windows for new consumers.

PollR is designed to solve this efficiently by consuming an arbitrary stream of data and partitioning the data to multiple client consumers efficiently using very simple code at the core (only a few hundred lines).

It is designed to be fast, efficient, and easy to understand (though not suitable for all scenarios such as segmenting lookback by partitions).

## Conceptual Overview

As the name suggests, PollR has two main concepts: polling and broadcasting.

The design is intentionally simple to allow it to be easy to understand rather than hyper optimization (e.g. ring buffer per partition).

### Stream Producers

Stream producers are functions (of any kind) that produce a stream of results (e.g. from a database, an API, etc.) that PollR can read from.  The producer is designed to be flexible and can produce data in any way that makes sense for the application.  The only requirement is that it produces data in a way that can be read forward using a cursor.

### Polling

The core of PollR is a poller which periodically reads forward from a data source.  The default `DateTimeOffset` poller starts from a **fixed look-back window**.  Once it has read the data, it continues forward using the last read position as a cursor.

> 💡 Therefore, your data must have an indexed, sequenced value to query on.

However, when a new consumer joins the stream, it can provide an older cursor up to a maximum look-back window.  This allows new consumers to synchronize with the current stream and the core poller only needs to **read the data once**.

Consumers join the stream by providing:

1. A partition that they are subscribing to
2. The most recent cursor that they have

The read cycle uses the current cursor unless a subscriber is catching up, in which case it reads from the oldest active catch-up cursor.  This allows the poller to efficiently read forward without needing to run a separate read for each consumer, but keep in mind that *it does not partition by consumer*; it uses a global feed.

> ℹ️ `PollRCaster` performs a global polling read and then distributes the results by subscribed partition while reading through all results.  This is most suitable for scenarios where consumers join occasionally, sync up, and stay attached to the feed for long durations.  When each partition needs its own cursor, failure domain, or independent polling cost, use `PartitionedPollRCaster` instead—see [Partitioned Poller](#partitioned-poller) in the Usage section.

### Broadcasting

Once the poller has read the data, it broadcasts the data to all of the consumers that are subscribed to the stream.  The single stream can contain **multiple partitions**.

The broadcast stage iterates through each value in the stream and identifies whether there are any consumers that are subscribed to the partition of the record.

If there are, it checks each consumer's cursor to determine whether the record is newer than what that consumer has already received.  If it is, it sends the record to the consumer and updates the consumer's cursor.

### Limitations

Because the poller reads from a global feed, it is not suitable for all scenarios.  Most notably, it performs best when the consumer pattern favors low number of joins (long connected sessions) since the newest join incurs the highest cost of lookback.  This implementation trades the read efficiency of a ring buffer for the memory efficiency of only relying on a polling read (which can scale at the DB layer using replicas).

> 💡 If per-partition lookback, failure isolation, or partition-specific polling cost is a priority, `PartitionedPollRCaster` addresses all of these directly.  See [Partitioned Poller](#partitioned-poller) in the Usage section.

## Key Entities

|Class|Description|
|---|---|
|`PollRBase<TData, TPartition, TCursor>`|The base poller that reads from a data source and broadcasts to consumers.  This is the core of the library and is designed to be extended for different types of data sources and cursors.|
|`PollRCaster<TData, TPartition>`|The default `DateTimeOffset` implementation of the poller.  This is designed to be used out of the box for most scenarios.|
|`PartitionedPollRBase<TData, TPartition, TCursor>`|The partition-aware base poller that schedules and polls active partitions independently.  Use this when cursor movement, retries, and failures need to stay local to each partition.|
|`PartitionedPollRCaster<TData, TPartition>`|The default `DateTimeOffset` partitioned implementation.  This is designed for scenarios where a shared global feed is not the right polling model.|
|`PollR.AspNetCore`|ASP.NET Core extensions for exposing `PollRCaster` and `PartitionedPollRCaster` as Server-Sent Events.|
|`IDataStream<TData>`|The interface that represents a stream of data that can be read from.  This is used by the poller to read data from the producer.|
|`ChannelDataStream<TData>`|A concrete implementation of `IDataStream<TData>` that uses a channel to buffer data between the producer and consumers.  Use this as the default|

## Building and Testing

```shell
# Build the entire solution
dotnet build

# Run TUnit tests
dotnet run --project src/PollR.Tests --disable-logo
```

### Console Demos

The console app contains three commands that showcase different scenarios:

```shell
# List the scenarios
dotnet run --project samples/console

# Run a basic two consumer demo
dotnet run --project samples/Console -- basic-two-consumer

# Run a multi-consumer demo
dotnet run --project samples/Console -- multiple-consumer

# Run in story mode that allows you to step through execution
# and understand how the poller behaves in different scenarios
dotnet run --project samples/Console -- story-mode
```

## Usage

### Global Feed Poller

`PollRCaster` reads from one shared data source and distributes records to subscribers by partition.  Use this when all partitions share a common query path and the workload favors long-lived subscribers that stay attached to the same feed.

Basic usage reading from a producer that produces `int` data, partitioned by `string`, and using a `DateTimeOffset` cursor:

```csharp
using PollR;

// Create records with a cursor and partition.
var records = new[]
{
    new ProducerResult<int, string>(1, DateTimeOffset.UtcNow, "tenant-1"),
};

// Create a poller from any async producer function.
var poller = new PollRCaster<int, string>(
    (cursor, cancellationToken) => ProduceAsync(cursor, cancellationToken, records)
);

// Subscribe to a partition and receive the bounded channel-backed stream.
var stream = poller.Subscribe("tenant-1", DateTimeOffset.UtcNow.AddMinutes(-1));

// Run one polling tick.
await poller.TickAsync();

// Read the data that was broadcast to the subscribed consumer.
while (stream.Reader.TryRead(out var item))
{
    if (item.TryGetData(out var result))
    {
        Console.WriteLine($"{result.Partition} {result.Cursor:O} {result.Data}");
    }
}

static async IAsyncEnumerable<ProducerResult<int, string>> ProduceAsync(
    DateTimeOffset cursor,
    CancellationToken cancellationToken,
    IEnumerable<ProducerResult<int, string>> records
)
{
    // Simulate async work, such as querying a database or API.
    await Task.Yield();

    // Return only records newer than the cursor PollR requested.
    foreach (var record in records.Where(record => record.Cursor > cursor))
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return record;
    }
}
```

> 💡 See the `samples/Console` for more examples.

### Partitioned Poller

`PartitionedPollRCaster` is the alternative when a **global feed is the wrong unit of work**.  Instead of reading one shared stream and distributing by partition after the fact, it treats each active partition as its own polling lane.

That changes the behavior in a few important ways:

1. Catch-up is local to the partition that asked for it.
2. Retry and failure stay local to the partition that failed.
3. Idle partitions stop polling until a subscriber joins again.
4. Different due partitions can poll concurrently up to the configured limit.

Use it when per-partition lookback, partition-local failure isolation, or partition-specific polling cost matters more than reading from one shared global feed.

```csharp
using PollR;

enum OrderProjection
{
    Summary
}

record OrderEvent(long Id, string Tenant, decimal Total, DateTimeOffset CreatedAt);

var poller = new PartitionedPollRCaster<OrderEvent, string>(
    async (tenant, cursor, cancellationToken) =>
    {
        // 👇 Poll one partition at a time instead of scanning a shared global feed.
        await foreach (var order in ReadOrdersAsync(tenant, cursor, cancellationToken))
        {
            // Each yielded record still carries data, cursor, and partition.
            yield return new ProducerResult<OrderEvent, string>(
                order,
                order.CreatedAt,
                tenant
            );
        }
    },
    // Each active partition uses the same base interval.
    pollingInterval: TimeSpan.FromSeconds(1),
    // Due partitions may poll together up to this limit.
    maxConcurrentPartitions: 4
)
// Registered projections still run once per item/key/partition and then fan out.
.RegisterProjection(
    OrderProjection.Summary,
    data => new { data.Data.Id, data.Data.Total }
);

var tenantOneStream = poller.Subscribe(
    "tenant-1",
    DateTimeOffset.UtcNow.AddMinutes(-1)
); // 🛜 Subscribe one partition directly.

var tenantTwoProjectionStream = poller.SubscribeProjection<OrderProjection, object>(
    OrderProjection.Summary,
    "tenant-2",
    DateTimeOffset.UtcNow.AddMinutes(-5) // 👈
); // A deeper lookback on tenant-2 does not rewind tenant-1.

await poller.TickAsync(); // ▶️ Run one tick across only the partitions that are currently due.

while (tenantOneStream.Reader.TryRead(out var item))
{
    if (item.TryGetData(out var result))
    {
        // Direct subscribers receive raw records.
        Console.WriteLine($"direct {result.Partition} {result.Cursor:O} {result.Data.Total}");
    }
}

while (tenantTwoProjectionStream.Reader.TryRead(out var item))
{
    if (item.TryGetData(out var result))
    {
        // Projection subscribers receive the registered shape.
        Console.WriteLine($"projection {result.Partition} {result.Cursor:O} {result.Data}");
    }
}
```

The ASP.NET SSE adapter works with the partitioned poller as well, so the same `ForHttp(...)` builder can expose partition-local streams over HTTP.

### ASP.NET Core

PollR is intended to be used with ASP.NET Core via Server-Sent Events (SSE).

This mechanism can replace a dedicated web socket implementation in some scenarios and obviate the need for additional backplane infrastructure to manage web socket connections.

This is achieved by using the HTTP/2 connection and using the data source (e.g. database) as the source of truth for the stream.

> 💡 In a server cluster, each node runs its own poller and serves the clients connected to that node.  This avoids running infrastructure such as a Redis backplane or SignalR scale-out provider, but it also means each node reads from the source of truth independently.  That tradeoff is often acceptable when the stream is already queryable by cursor and partition.  For database-backed streams, point pollers at read replicas when possible so fan-out traffic scales across replicas instead of adding unnecessary load to the primary database.

```csharp
using System.Text.Json;
using PollR.AspNetCore;

app.MapGet("/events/{tenant}", (string tenant, IHttpContextAccessor httpContextAccessor) =>
    poller
        // Read Last-Event-ID from the current request and subscribe this client to the tenant partition.
        .ForHttp(httpContextAccessor)
        .WithSubscription(tenant, DateTimeOffset.UtcNow.AddMinutes(-1))
        // Use the partition as the SSE event type.
        .WithSseEventType(partition => partition)
        // Use an endpoint-local projection. This is safe for request-specific payloads.
        .WithAdHocSerializedProjection(data =>
        {
            var payload = JsonSerializer.Serialize(data.Data);
            return payload;
        })
);

app.MapGet("/events-typed/{tenant}", (string tenant, IHttpContextAccessor httpContextAccessor) =>
    poller
        // Read Last-Event-ID from the current request and subscribe this client to the tenant partition.
        .ForHttp(httpContextAccessor)
        .WithSubscription(tenant, DateTimeOffset.UtcNow.AddMinutes(-1))
        // Use the partition as the SSE event type.
        .WithSseEventType(partition => partition)
        // Pass typed data through; ASP.NET Core serializes it for the SSE response.
        .WithProjection(payload => payload.Data)
);
```

Browser clients can consume the stream with `EventSource`:

```js
const tenant = "tenant-1";
const events = new EventSource(`/events/${encodeURIComponent(tenant)}`);

// If the server writes "event: tenant-1", listen for that named event.
events.addEventListener(tenant, event => {
  const message = JSON.parse(event.data);
  console.log("received", event.lastEventId, message);
});

events.onerror = () => {
  // The browser reconnects automatically and sends Last-Event-ID.
  console.warn("stream disconnected; waiting for reconnect");
};

// Call this when the page no longer needs updates.
// events.close();
```

> `EventSource` reconnects automatically. PollR uses `Last-Event-ID` to resume from the last cursor the browser received. Native `EventSource` does not support custom request headers, so browser authentication usually uses cookies or URL-scoped tokens.

#### Partitioned Poller with ASP.NET Core

The `ForHttp(...)` call, subscription builder, SSE options, and browser-side `EventSource` setup are identical to the global feed version above.  The only difference is the poller type — `PartitionedPollRCaster` produces a partition-local stream per subscriber instead of filtering a shared global feed.

```csharp
using PollR.AspNetCore;

// Same ForHttp builder surface; works because PartitionedPollRCaster implements
// the same shared subscriber contract as PollRCaster.
app.MapGet("/events/{tenant}", (string tenant, IHttpContextAccessor httpContextAccessor) =>
    partitionedPoller // 👈 only thing that changed
        .ForHttp(httpContextAccessor)
        .WithSubscription(tenant, DateTimeOffset.UtcNow.AddMinutes(-1))
        .WithSseEventType(partition => partition)
        .WithRegisteredSerializedProjection(MessageProjection.Full) // same projection API
);
```

> 💡 Registered projections, ad-hoc projections, and `WithSseEventType` all work exactly the same way.

### Registered Projections vs Ad-Hoc Projections

Registered projections name stable, shareable payload shapes on the poller:

```csharp
enum MessageProjection
{
    Full,
    Summary,
}

var poller = new PollRCaster<MessageEvent, string>(ProduceAsync)
    // 👇 Register a single serializer that produce a shared payload (reduce fan-out cost)
    .RegisterSerializedProjection(
        MessageProjection.Full,
        payload => JsonSerializer.Serialize(payload)
    )
    // 👇 Only registers a projection
    .RegisterProjection(
        MessageProjection.Summary,
        payload => new MessageSummary(payload.Data.Id, payload.Data.Text)
    );

record MessageSummary(long Id, string Text);
```

Endpoints can use registered projections by `enum` or string key:

```csharp
app.MapGet("/events/{tenant}", (string tenant, IHttpContextAccessor httpContextAccessor) =>
    poller
        .ForHttp(httpContextAccessor)
        .WithSubscription(tenant, DateTimeOffset.UtcNow.AddMinutes(-1))
        .WithRegisteredSerializedProjection(MessageProjection.Full)
);

app.MapGet("/events/{tenant}/summary", (string tenant, IHttpContextAccessor httpContextAccessor) =>
    poller
        .ForHttp(httpContextAccessor)
        .WithSubscription(tenant, DateTimeOffset.UtcNow.AddMinutes(-1))
        .WithRegisteredProjection<MessageProjection, MessageSummary>(MessageProjection.Summary)
);
```

**Registered typed projections** run once per record per projection group. ASP.NET Core still serializes the typed payload per connected client.

**Registered serialized projections** run once per record per projection group and reuse the serialized string for matching subscribers. Prefer this for high fan-out SSE payloads. For maximum throughput, use `System.Text.Json` source-generated serializers inside the registered serializer.

```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

var poller = new PollRCaster<MessageEvent, string>(ProduceAsync)
    // 👇 A produced message serializes only one time and is shared by all consumers
    .RegisterSerializedProjection(
        MessageProjection.Full,
        payload => JsonSerializer.Serialize(payload.Data, PollRJsonContext.Default.MessageEvent)
    );

// 👇 Use source generated serializer for better perf!
[JsonSerializable(typeof(MessageEvent))]
partial class PollRJsonContext : JsonSerializerContext;
```

**Ad-hoc projections** run per subscriber. Use them when the payload depends on request, user, query, or other subscriber-local state:

```csharp
app.MapGet("/events/{tenant}/text", (string tenant, IHttpContextAccessor httpContextAccessor) =>
    poller
        .ForHttp(httpContextAccessor)
        .WithSubscription(tenant, DateTimeOffset.UtcNow.AddMinutes(-1))
        .WithAdHocSerializedProjection(
            payload => JsonSerializer.Serialize(new { payload.Data.Text })
        )
);
```

## Web Application Demos

The repository includes two small ASP.NET Core demos that use [EF Core InMemory](https://learn.microsoft.com/en-us/ef/core/providers/in-memory/?tabs=dotnet-core-cli) as the stream source of truth:

```shell
# Run the minimal API demo on port 5055.
dotnet run --project samples/WebMinimal --urls http://localhost:5055

# Or run the controller-based demo on port 5055.
dotnet run --project samples/WebController --urls http://localhost:5055
```

Each demo exposes the same two endpoints:

```shell
# Subscribe to the "general" topic.
# -N disables curl buffering so SSE events appear as soon as the server writes them.
curl -N http://localhost:5055/events/general

curl -N http://localhost:5055/events/{topic-goes-here}
```

```shell
# In another terminal, write a message into the EF InMemory database.
# The singleton poller reads the row and fans it out to the open SSE subscription.
curl -X POST http://localhost:5055/messages/general \
  -H "Content-Type: application/json" \
  -d '{"text":"hello from curl"}'

curl -X POST http://localhost:5055/messages/{topic-goes-here} \
  -H "Content-Type: application/json" \
  -d '{"text":"hello from curl"}'
```

### Using `curl` for SSE Reconnect Testing

`curl` keeps the SSE connection open until the server closes it or you stop it with `Ctrl+C`.

```shell
# Reconnect from a known cursor. The server reads Last-Event-ID and starts from that cursor.
curl -N \
  -H "Last-Event-ID: 2026-05-30T22:13:41.4302070+00:00" \
  http://localhost:5055/events/general
```
