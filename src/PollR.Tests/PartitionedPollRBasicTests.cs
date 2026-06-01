namespace PollR.Tests;

public class PartitionedPollRBasicTests
{
    [Test]
    public async Task TickAsync_ReceivesMixedPartitions_DeliversOnlySubscribedPartition()
    {
        // Guards partition-local fan-out; only the subscribed partition receives records.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PartitionedPollRCaster<int, string>(
            CreateProducer(
                ("tenant-1", new ProducerResult<int, string>(1, timestamp, "tenant-1")),
                ("tenant-2", new ProducerResult<int, string>(2, timestamp, "tenant-2"))
            )
        );
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1), stream);

        await PollR.TickAsync();

        await Assert.That(stream.DataResults()).IsEquivalentTo([1]);
    }

    [Test]
    public async Task TickAsync_CatchUpOnOnePartition_DoesNotMoveOtherPartitionCursor()
    {
        // Guards partition-local catch-up; one partition rewinds without affecting another.
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
        );

        PollR.Subscribe(
            "tenant-1",
            currentTimestamp.AddSeconds(-1),
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>()
        );
        PollR.Subscribe(
            "tenant-2",
            currentTimestamp.AddSeconds(-1),
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>()
        );
        await PollR.TickAsync();

        PollR.Subscribe(
            "tenant-1",
            catchUpCursor,
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>()
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
    public async Task TickAsync_ProducerThrows_BroadcastsErrorOnlyToFailedPartition()
    {
        // Guards failure isolation; one failing partition does not poison unrelated subscribers.
        var PollR = new PartitionedPollRCaster<int, string>(ThrowingProducer);
        var failingStream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();
        var healthyStream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-fail", DateTimeOffset.UtcNow, failingStream);
        PollR.Subscribe("tenant-ok", DateTimeOffset.UtcNow, healthyStream);

        await PollR.TickAsync();

        await Assert.That(failingStream.Errors()).IsEquivalentTo(["producer failed"]);
        await Assert.That(healthyStream.Errors()).IsEmpty();
    }

    [Test]
    public async Task TickAsync_DuePartitions_RunConcurrentlyUpToConfiguredLimit()
    {
        // Guards bounded concurrency; different partitions may poll together when budget allows.
        var allEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var activePolls = 0;
        var maxObservedPolls = 0;
        var enteredPartitions = 0;
        var PollR = new PartitionedPollRCaster<int, string>(
            ProduceAsync,
            maxConcurrentPartitions: 2
        );

        PollR.Subscribe(
            "tenant-1",
            DateTimeOffset.UtcNow,
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>()
        );
        PollR.Subscribe(
            "tenant-2",
            DateTimeOffset.UtcNow,
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>()
        );

        var tickTask = PollR.TickAsync();

        await allEntered.Task;
        release.SetResult();
        await tickTask;

        await Assert.That(maxObservedPolls).IsEqualTo(2);

        async IAsyncEnumerable<ProducerResult<int, string>> ProduceAsync(
            string partition,
            DateTimeOffset cursor,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
                CancellationToken cancellationToken
        )
        {
            _ = partition;
            _ = cursor;

            var currentPolls = Interlocked.Increment(ref activePolls);
            maxObservedPolls = Math.Max(maxObservedPolls, currentPolls);

            if (Interlocked.Increment(ref enteredPartitions) == 2)
            {
                allEntered.TrySetResult();
            }

            await release.Task.WaitAsync(cancellationToken);
            Interlocked.Decrement(ref activePolls);
            yield break;
        }
    }

    [Test]
    public async Task Subscribe_BeforeStartAsync_StagesJoinAndRegistersOnLaterTick()
    {
        // Guards lifecycle parity with the existing poller; Subscribe stays valid before StartAsync.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PartitionedPollRCaster<int, string>(
            CreateProducer(("tenant-1", new ProducerResult<int, string>(1, timestamp, "tenant-1")))
        );
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1), stream);

        await PollR.TickAsync();

        await Assert.That(stream.DataResults()).IsEquivalentTo([1]);
    }

    [Test]
    public async Task DisposeAsync_CompletesSubscribersOnce()
    {
        // Guards partitioned cleanup semantics; disposal completes active streams exactly once.
        var PollR = new PartitionedPollRCaster<int, string>(CreateProducer());
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-1", DateTimeOffset.UtcNow, stream);
        await PollR.TickAsync();

        await PollR.DisposeAsync();
        await PollR.DisposeAsync();

        await Assert.That(stream.StreamCompletedResults().Count).IsEqualTo(1);
        await Assert.That(stream.CompleteCount).IsEqualTo(1);
    }

    static PartitionDataProducer<int, string> CreateProducer(
        params (string Partition, ProducerResult<int, string> Result)[] partitionResults
    ) =>
        (partition, cursor, cancellationToken) =>
            ProducePartitionRecordsAfterCursorAsync(
                partition,
                cursor,
                cancellationToken,
                [
                    .. partitionResults
                        .Where(entry => entry.Partition == partition)
                        .Select(entry => entry.Result)
                ]
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
}
