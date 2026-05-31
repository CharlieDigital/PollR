using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using PollR.AspNetCore;

namespace PollR.Tests;

public class PollRAspNetCoreTests
{
    [Test]
    public async Task SubscribeSerializedServerSentEvents_UsesLastEventId_EmitsSerializedSseItem()
    {
        // Guards reconnect behavior; Last-Event-ID becomes the subscriber cursor and SSE metadata is emitted.
        var firstTimestamp = DateTimeOffset.UtcNow;
        var secondTimestamp = firstTimestamp.AddSeconds(1);
        var PollR = new PollRCaster<int, string>(
            CreateProducer(
                new ProducerResult<int, string>(1, firstTimestamp, "tenant-1"),
                new ProducerResult<int, string>(2, secondTimestamp, "tenant-1")
            )
        );
        var httpContext = CreateHttpContext();
        var httpContextAccessor = new HttpContextAccessor { HttpContext = httpContext };
        httpContext.Request.Headers["Last-Event-ID"] = firstTimestamp.ToString("O");

        var result = PollR
            .ForHttp(httpContextAccessor)
            .WithSubscription("tenant-1", firstTimestamp.AddMinutes(-1))
            .WithSseEventType(partition => partition)
            .WithAdHocSerializedProjection(data => $$"""{"value":{{data.Data}}}""");

        var executeTask = result.ExecuteAsync(httpContext);

        await PollR.TickAsync();
        await PollR.StopAsync();
        await executeTask;

        var body = ReadBody(httpContext);

        await Assert.That(body).DoesNotContain("value\":1");
        await Assert.That(body).Contains($"id: {secondTimestamp:O}");
        await Assert.That(body).Contains("event: tenant-1");
        await Assert.That(body).Contains("retry: 3000");
        await Assert.That(body).Contains("""data: {"value":2}""");
    }

    [Test]
    public async Task SubscribeServerSentEvents_MapsData_EmitsTypedSseItem()
    {
        // Guards typed adapter behavior; mapped payloads are written through ASP.NET's SSE result.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        );
        var httpContext = CreateHttpContext();
        var httpContextAccessor = new HttpContextAccessor { HttpContext = httpContext };

        var result = PollR
            .ForHttp(httpContextAccessor)
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithSseEventType(partition => partition)
            .WithProjection(data => new TestPayload(data.Data));

        var executeTask = result.ExecuteAsync(httpContext);

        await PollR.TickAsync();
        await PollR.StopAsync();
        await executeTask;

        var body = ReadBody(httpContext);

        await Assert.That(body).Contains($"id: {timestamp:O}");
        await Assert.That(body).Contains("event: tenant-1");
        await Assert.That(body).Contains("data: {\"value\":1}");
    }

    [Test]
    public async Task WithOptions_ConfiguresRetryInterval()
    {
        // Guards builder options; callers can configure SSE retry metadata fluently.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        );
        var httpContext = CreateHttpContext();
        var httpContextAccessor = new HttpContextAccessor { HttpContext = httpContext };

        var result = PollR
            .ForHttp(httpContextAccessor)
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithOptions(reconnectionInterval: TimeSpan.FromSeconds(10))
            .WithAdHocSerializedProjection(data => data.Data.ToString());

        var executeTask = result.ExecuteAsync(httpContext);

        await PollR.TickAsync();
        await PollR.StopAsync();
        await executeTask;

        var body = ReadBody(httpContext);

        await Assert.That(body).Contains("retry: 10000");
    }

    [Test]
    public async Task WithRegisteredSerializedProjection_UsesRegisteredProjection_EmitsSerializedSseItem()
    {
        // Guards shared projection identity; endpoints can select a projection registered once on the poller.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        )
            .RegisterSerializedProjection("value-v1", data => $$"""{"value":{{data.Data}}}""")
            .RegisterSerializedProjection("double-v1", data => $$"""{"value":{{data.Data * 2}}}""");
        var httpContext = CreateHttpContext();
        var httpContextAccessor = new HttpContextAccessor { HttpContext = httpContext };

        var result = PollR
            .ForHttp(httpContextAccessor)
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithSseEventType(partition => partition)
            .WithRegisteredSerializedProjection("double-v1");

        var executeTask = result.ExecuteAsync(httpContext);

        await PollR.TickAsync();
        await PollR.StopAsync();
        await executeTask;

        var body = ReadBody(httpContext);

        await Assert.That(body).Contains("""data: {"value":2}""");
    }

    [Test]
    public async Task WithRegisteredSerializedProjection_WithEnumKey_UsesRegisteredProjection()
    {
        // Guards typed projection identity; enum keys translate to full string keys for registration and lookup.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        )
            .RegisterSerializedProjection(
                TestProjection.Value,
                data => $$"""{"value":{{data.Data}}}"""
            )
            .RegisterSerializedProjection(
                TestProjection.Double,
                data => $$"""{"value":{{data.Data * 2}}}"""
            );
        var httpContext = CreateHttpContext();
        var httpContextAccessor = new HttpContextAccessor { HttpContext = httpContext };

        var result = PollR
            .ForHttp(httpContextAccessor)
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithRegisteredSerializedProjection(TestProjection.Double);

        var executeTask = result.ExecuteAsync(httpContext);

        await PollR.TickAsync();
        await PollR.StopAsync();
        await executeTask;

        var body = ReadBody(httpContext);

        await Assert.That(body).Contains("""data: {"value":2}""");
    }

    [Test]
    public async Task WithRegisteredSerializedProjection_MultipleSubscribers_SerializerRunsOncePerRecord()
    {
        // Guards registered SSE performance; shared serialized projections run once for matching subscribers.
        var timestamp = DateTimeOffset.UtcNow;
        var serializationCount = 0;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        ).RegisterSerializedProjection(
            TestProjection.Value,
            data =>
            {
                serializationCount++;
                return $$"""{"value":{{data.Data}}}""";
            }
        );
        var firstHttpContext = CreateHttpContext();
        var secondHttpContext = CreateHttpContext();

        var firstResult = PollR
            .ForHttp(new HttpContextAccessor { HttpContext = firstHttpContext })
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithRegisteredSerializedProjection(TestProjection.Value);
        var secondResult = PollR
            .ForHttp(new HttpContextAccessor { HttpContext = secondHttpContext })
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithRegisteredSerializedProjection(TestProjection.Value);

        var firstExecuteTask = firstResult.ExecuteAsync(firstHttpContext);
        var secondExecuteTask = secondResult.ExecuteAsync(secondHttpContext);

        await PollR.TickAsync();
        await PollR.StopAsync();
        await Task.WhenAll(firstExecuteTask, secondExecuteTask);

        await Assert.That(serializationCount).IsEqualTo(1);
        await Assert.That(ReadBody(firstHttpContext)).Contains("""data: {"value":1}""");
        await Assert.That(ReadBody(secondHttpContext)).Contains("""data: {"value":1}""");
    }

    [Test]
    public async Task WithAdHocSerializedProjection_MultipleSubscribers_SerializerRunsPerSubscriber()
    {
        // Guards ad-hoc semantics; endpoint-local projections still run per subscriber.
        var timestamp = DateTimeOffset.UtcNow;
        var serializationCount = 0;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        );
        var firstHttpContext = CreateHttpContext();
        var secondHttpContext = CreateHttpContext();

        string Serialize(DataResult<int, string, DateTimeOffset> data)
        {
            serializationCount++;
            return $$"""{"value":{{data.Data}}}""";
        }

        var firstResult = PollR
            .ForHttp(new HttpContextAccessor { HttpContext = firstHttpContext })
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithAdHocSerializedProjection(Serialize);
        var secondResult = PollR
            .ForHttp(new HttpContextAccessor { HttpContext = secondHttpContext })
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithAdHocSerializedProjection(Serialize);

        var firstExecuteTask = firstResult.ExecuteAsync(firstHttpContext);
        var secondExecuteTask = secondResult.ExecuteAsync(secondHttpContext);

        await PollR.TickAsync();
        await PollR.StopAsync();
        await Task.WhenAll(firstExecuteTask, secondExecuteTask);

        await Assert.That(serializationCount).IsEqualTo(2);
    }

    [Test]
    public async Task WithRegisteredProjection_EmitsTypedSseItem()
    {
        // Guards registered typed projection; core shapes once and ASP.NET writes typed SSE payloads.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        ).RegisterProjection(TestProjection.Value, data => new TestPayload(data.Data));
        var httpContext = CreateHttpContext();
        var httpContextAccessor = new HttpContextAccessor { HttpContext = httpContext };

        var result = PollR
            .ForHttp(httpContextAccessor)
            .WithSubscription("tenant-1", timestamp.AddMinutes(-1))
            .WithRegisteredProjection<TestProjection, TestPayload>(TestProjection.Value);

        var executeTask = result.ExecuteAsync(httpContext);

        await PollR.TickAsync();
        await PollR.StopAsync();
        await executeTask;

        var body = ReadBody(httpContext);

        await Assert.That(body).Contains("data: {\"value\":1}");
    }

    [Test]
    public async Task RegisterSerializedProjection_DuplicateKey_ThrowsInvalidOperationException()
    {
        // Guards projection identity; each key names one deterministic shared projection.
        var PollR = new PollRCaster<int, string>(CreateProducer());

        PollR.RegisterSerializedProjection("value-v1", data => data.Data.ToString());

        await Assert
            .That(() =>
                PollR.RegisterSerializedProjection("value-v1", data => data.Data.ToString())
            )
            .Throws<InvalidOperationException>();
    }

    static DefaultHttpContext CreateHttpContext()
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Response.Body = new MemoryStream();
        httpContext.RequestServices = new ServiceCollection().BuildServiceProvider();
        return httpContext;
    }

    static string ReadBody(HttpContext httpContext)
    {
        httpContext.Response.Body.Position = 0;
        using var reader = new StreamReader(
            httpContext.Response.Body,
            Encoding.UTF8,
            detectEncodingFromByteOrderMarks: false,
            leaveOpen: true
        );
        return reader.ReadToEnd();
    }

    static DataProducer<int, string> CreateProducer(params ProducerResult<int, string>[] results) =>
        (cursor, cancellationToken) =>
            ProduceRecordsAfterCursorAsync(cursor, cancellationToken, results);

    static async IAsyncEnumerable<ProducerResult<int, string>> ProduceRecordsAfterCursorAsync(
        DateTimeOffset cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken,
        params ProducerResult<int, string>[] results
    )
    {
        await Task.Yield();

        foreach (var result in results.Where(result => result.Cursor > cursor))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return result;
        }
    }

    sealed record TestPayload(int Value);

    enum TestProjection
    {
        Value,
        Double,
    }
}
