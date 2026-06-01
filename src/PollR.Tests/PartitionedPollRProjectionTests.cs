namespace PollR.Tests;

public class PartitionedPollRProjectionTests
{
    [Test]
    public async Task SubscribeProjection_MultipleSubscribers_ProjectionRunsOncePerRecord()
    {
        // Guards shared projection fan-out within one partition.
        var timestamp = DateTimeOffset.UtcNow;
        var projectionCount = 0;
        var PollR = new PartitionedPollRCaster<int, string>(
            CreateProducer(("tenant-1", new ProducerResult<int, string>(1, timestamp, "tenant-1")))
        ).RegisterProjection(
            TestProjection.Value,
            data =>
            {
                projectionCount++;
                return data.Data * 10;
            }
        );

        var firstStream = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-1",
            timestamp.AddSeconds(-1)
        );
        var secondStream = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-1",
            timestamp.AddSeconds(-1)
        );

        await PollR.TickAsync();

        await Assert.That(projectionCount).IsEqualTo(1);
        await Assert.That(await ReadDataAsync(firstStream)).IsEqualTo(10);
        await Assert.That(await ReadDataAsync(secondStream)).IsEqualTo(10);
    }

    [Test]
    public async Task SubscribeProjection_CatchUpOnOnePartition_DoesNotMoveOtherPartitionCursor()
    {
        // Guards partition-local projection catch-up; one projection subscriber does not rewind other partitions.
        var currentTimestamp = DateTimeOffset.UtcNow;
        var catchUpCursor = currentTimestamp.AddSeconds(-30);
        var requestedCursors = new List<(string Partition, DateTimeOffset Cursor)>();
        var PollR = new PartitionedPollRCaster<int, string>(
            (partition, cursor, cancellationToken) =>
                RecordPartitionCursorAndProduceAsync(
                    requestedCursors,
                    partition,
                    cursor,
                    cancellationToken,
                    new ProducerResult<int, string>(
                        partition == "tenant-1" ? 1 : 2,
                        currentTimestamp,
                        partition
                    )
                ),
            pollingInterval: TimeSpan.Zero
        ).RegisterProjection(TestProjection.Value, data => data.Data);

        _ = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-1",
            currentTimestamp.AddSeconds(-1)
        );
        _ = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-2",
            currentTimestamp.AddSeconds(-1)
        );
        await PollR.TickAsync();

        _ = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-1",
            catchUpCursor,
            CancellationToken.None
        );
        await PollR.TickAsync();

        var secondTenantOneCursor = requestedCursors
            .Where(entry => entry.Partition == "tenant-1")
            .Select(entry => entry.Cursor)
            .ElementAt(1);
        var secondTenantTwoCursor = requestedCursors
            .Where(entry => entry.Partition == "tenant-2")
            .Select(entry => entry.Cursor)
            .ElementAt(1);

        await Assert.That(secondTenantOneCursor).IsEqualTo(catchUpCursor);
        await Assert.That(secondTenantTwoCursor).IsEqualTo(currentTimestamp);
    }

    [Test]
    public async Task TickAsync_ProjectionProducerThrows_BroadcastsErrorOnlyToFailedPartition()
    {
        // Guards partition-local projection failure fan-out.
        var PollR = new PartitionedPollRCaster<int, string>(ThrowingProducer).RegisterProjection(
            TestProjection.Value,
            data => data.Data
        );
        var failingStream = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-fail",
            DateTimeOffset.UtcNow
        );
        var healthyStream = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-ok",
            DateTimeOffset.UtcNow
        );

        await PollR.TickAsync();

        var failingItem = await failingStream.Reader.ReadAsync();

        await Assert.That(failingItem.TryGetError(out var error)).IsTrue();
        await Assert.That(error.ErrorMessage).IsEqualTo("producer failed");
        await Assert.That(healthyStream.Reader.Count).IsEqualTo(0);
    }

    [Test]
    public async Task SubscribeSerializedProjection_MultipleSubscribers_SerializerRunsOncePerRecord()
    {
        // Guards registered serialized projection amortization within one partition.
        var timestamp = DateTimeOffset.UtcNow;
        var serializationCount = 0;
        var PollR = new PartitionedPollRCaster<int, string>(
            CreateProducer(("tenant-1", new ProducerResult<int, string>(1, timestamp, "tenant-1")))
        ).RegisterSerializedProjection(
            TestProjection.Value,
            data =>
            {
                serializationCount++;
                return $$"""{"value":{{data.Data}}}""";
            }
        );

        var firstStream = PollR.SubscribeSerializedProjection(
            TestProjection.Value,
            "tenant-1",
            timestamp.AddSeconds(-1)
        );
        var secondStream = PollR.SubscribeSerializedProjection(
            TestProjection.Value,
            "tenant-1",
            timestamp.AddSeconds(-1)
        );

        await PollR.TickAsync();

        await Assert.That(serializationCount).IsEqualTo(1);
        await Assert.That(await ReadDataAsync(firstStream)).IsEqualTo("""{"value":1}""");
        await Assert.That(await ReadDataAsync(secondStream)).IsEqualTo("""{"value":1}""");
    }

    static PartitionDataProducer<int, string> CreateProducer(
        params (string Partition, ProducerResult<int, string> Result)[] partitionResults
    ) =>
        (partition, cursor, cancellationToken) =>
            ProducePartitionRecordsAfterCursorAsync(
                partition,
                cursor,
                cancellationToken,
                partitionResults
                    .Where(entry => entry.Partition == partition)
                    .Select(entry => entry.Result)
                    .ToArray()
            );

    static async IAsyncEnumerable<
        ProducerResult<int, string>
    > ProducePartitionRecordsAfterCursorAsync(
        string partition,
        DateTimeOffset cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken,
        params ProducerResult<int, string>[] results
    )
    {
        await Task.Yield();

        foreach (
            var result in results.Where(result =>
                result.Partition == partition && result.Cursor > cursor
            )
        )
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return result;
        }
    }

    static async IAsyncEnumerable<ProducerResult<int, string>> RecordPartitionCursorAndProduceAsync(
        List<(string Partition, DateTimeOffset Cursor)> cursors,
        string partition,
        DateTimeOffset cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken,
        params ProducerResult<int, string>[] results
    )
    {
        cursors.Add((partition, cursor));

        await foreach (
            var result in ProducePartitionRecordsAfterCursorAsync(
                partition,
                cursor,
                cancellationToken,
                results
            )
        )
        {
            yield return result;
        }
    }

    static async IAsyncEnumerable<ProducerResult<int, string>> ThrowingProducer(
        string partition,
        DateTimeOffset cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        _ = cursor;
        _ = cancellationToken;

        await Task.Yield();

        if (partition == "tenant-fail")
        {
            throw new InvalidOperationException("producer failed");
        }

        yield break;
    }

    static async Task<TPayload> ReadDataAsync<TPayload>(
        ChannelDataStream<IntervalData<TPayload, string, DateTimeOffset>> stream
    )
    {
        var item = await stream.Reader.ReadAsync();

        return item.TryGetData(out var data)
            ? data.Data
            : throw new InvalidOperationException("Expected a data result.");
    }

    enum TestProjection
    {
        Value,
    }
}
