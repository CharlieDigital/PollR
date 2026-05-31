namespace PollR.Tests;

public class PollRBasicTests
{
    [Test]
    public async Task TickAsync_ReceivesMixedPartitions_DeliversOnlySubscribedPartition()
    {
        // Guards partition fan-out; one tick should send matching partition records and ignore other partitions.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(
                new ProducerResult<int, string>(1, timestamp, "tenant-1"),
                new ProducerResult<int, string>(2, timestamp, "tenant-2")
            )
        );
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1), stream);

        await PollR.TickAsync();

        await Assert.That(stream.DataResults()).IsEquivalentTo([1]);
    }

    [Test]
    public async Task TickAsync_ReplaysSameProducerRecord_DoesNotDeliverDuplicate()
    {
        // Guards cursor advancement; once a subscriber receives a record, the same timestamp is not sent again.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        );
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1), stream);

        await PollR.TickAsync();
        await PollR.TickAsync();

        await Assert.That(stream.DataResults()).IsEquivalentTo([1]);
    }

    [Test]
    public async Task TickAsync_RegistersCatchUpSubscriber_UsesSubscriberCursorForRead()
    {
        // Guards catch-up reads; a subscriber joining behind the stream moves the next manual tick back.
        var currentTimestamp = DateTimeOffset.UtcNow;
        var catchUpCursor = currentTimestamp.AddSeconds(-30);
        var requestedCursors = new List<DateTimeOffset>();

        var PollR = new PollRCaster<int, string>(
            (cursor, cancellationToken) =>
                RecordCursorsAndProduceAsync(
                    requestedCursors,
                    cursor,
                    cancellationToken,
                    requestedCursors.Count == 0
                        ? [new ProducerResult<int, string>(1, currentTimestamp, "tenant-1")]
                        : []
                )
        );

        await PollR.TickAsync();
        PollR.Subscribe(
            "tenant-1",
            catchUpCursor,
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>()
        );
        await PollR.TickAsync();

        await Assert.That(requestedCursors[1]).IsEqualTo(catchUpCursor);
    }

    [Test]
    public async Task TickAsync_RunsAfterCatchUpSubscribe_DeliversCatchUpRecords()
    {
        // Guards externally controlled catch-up; a manual tick after Subscribe delivers replay records immediately.
        var currentTimestamp = DateTimeOffset.UtcNow;
        var catchUpTimestamp = currentTimestamp.AddSeconds(-30);
        var PollR = new PollRCaster<int, string>(
            (cursor, cancellationToken) =>
                ProduceRecordsAfterCursorAsync(
                    cursor,
                    cancellationToken,
                    new ProducerResult<int, string>(2, catchUpTimestamp, "tenant-1"),
                    new ProducerResult<int, string>(1, currentTimestamp, "tenant-1")
                )
        );
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        await PollR.TickAsync();
        PollR.Subscribe("tenant-1", catchUpTimestamp.AddSeconds(-1), stream);
        await PollR.TickAsync();

        await Assert.That(stream.DataResults()).IsEquivalentTo([1, 2]);
    }

    [Test]
    public async Task TickAsync_ProducerThrows_BroadcastsErrorResultToSubscribers()
    {
        // Guards producer failure behavior; exceptions surface to subscribers as ErrorResult on the same tick.
        var PollR = new PollRCaster<int, string>(ThrowingProducer);
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-1", DateTimeOffset.UtcNow, stream);

        await PollR.TickAsync();

        await Assert.That(stream.Errors()).IsEquivalentTo(["producer failed"]);
    }

    [Test]
    public async Task TickAsync_ChannelDataStreamSubscribed_DeliversDataThroughReader()
    {
        // Guards default stream integration; the built-in channel stream receives tick output through its reader.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        );
        var stream = DefaultChannelDataStream<int, string>.CreateUnbounded();

        PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1), stream);

        await PollR.TickAsync();
        var item = await stream.Reader.ReadAsync();

        await Assert.That(item.TryGetData(out var data)).IsTrue();
        await Assert.That(data.Data).IsEqualTo(1);
    }

    [Test]
    public async Task TickAsync_DeliversDataResult_IncludesCursorAndPartition()
    {
        // Guards SSE adapter metadata; subscribers receive the cursor and partition with each data item.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        );
        var stream = PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1));

        await PollR.TickAsync();
        var item = await stream.Reader.ReadAsync();

        await Assert.That(item.TryGetData(out var data)).IsTrue();
        await Assert.That(data.Data).IsEqualTo(1);
        await Assert.That(data.Cursor).IsEqualTo(timestamp);
        await Assert.That(data.Partition).IsEqualTo("tenant-1");
    }

    [Test]
    public async Task Subscribe_WithoutStream_ReturnsSubscribedBoundedChannelDataStream()
    {
        // Guards convenience subscription; callers receive a bounded stream without creating one first.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        );

        var stream = PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1));

        await PollR.TickAsync();
        var item = await stream.Reader.ReadAsync();

        await Assert.That(item.TryGetData(out var data)).IsTrue();
        await Assert.That(data.Data).IsEqualTo(1);
    }

    [Test]
    public async Task Subscribe_WithStreamCapacity_DropsNewWritesWhenReturnedStreamIsFull()
    {
        // Guards default SSE-style subscription; convenience streams use bounded drop-write behavior.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(
                new ProducerResult<int, string>(1, timestamp, "tenant-1"),
                new ProducerResult<int, string>(2, timestamp.AddTicks(1), "tenant-1")
            )
        );

        var stream = PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1), streamCapacity: 1);

        await PollR.TickAsync();

        await Assert.That(stream.Reader.Count).IsEqualTo(1);

        var item = await stream.Reader.ReadAsync();

        await Assert.That(item.TryGetData(out var data)).IsTrue();
        await Assert.That(data.Data).IsEqualTo(1);
    }

    [Test]
    public async Task Subscribe_WithDefaultAndOversizedStreamCapacity_UsesDefaultAndClampsMax()
    {
        // Guards production heap limits; caller-provided stream capacities cannot exceed the max buffer size.
        var timestamp = DateTimeOffset.UtcNow;
        var results = Enumerable
            .Range(1, 200)
            .Select(index => new ProducerResult<int, string>(
                index,
                timestamp.AddTicks(index),
                "tenant-1"
            ))
            .ToArray();
        var PollR = new PollRCaster<int, string>(CreateProducer(results));

        var defaultStream = PollR.Subscribe("tenant-1", timestamp);
        var oversizedStream = PollR.Subscribe("tenant-1", timestamp, streamCapacity: 10_000);

        await PollR.TickAsync();

        await Assert.That(defaultStream.Reader.Count).IsEqualTo(64);
        await Assert.That(oversizedStream.Reader.Count).IsEqualTo(128);
    }

    [Test]
    public async Task SubscribeProjection_MultipleSubscribers_ProjectionRunsOncePerRecord()
    {
        // Guards registered projection fan-out; one projected payload is shared across subscribers.
        var timestamp = DateTimeOffset.UtcNow;
        var projectionCount = 0;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
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
    public async Task SubscribeSerializedProjection_MultipleSubscribers_SerializerRunsOncePerRecord()
    {
        // Guards registered serialized fan-out; expensive serialization is paid once per record.
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

    [Test]
    public async Task SubscribeProjection_DifferentProjectionKeys_RunOncePerKey()
    {
        // Guards projection grouping; each registered key computes its own payload once.
        var timestamp = DateTimeOffset.UtcNow;
        var valueCount = 0;
        var doubleCount = 0;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(new ProducerResult<int, string>(1, timestamp, "tenant-1"))
        )
            .RegisterProjection(
                TestProjection.Value,
                data =>
                {
                    valueCount++;
                    return data.Data;
                }
            )
            .RegisterProjection(
                TestProjection.Double,
                data =>
                {
                    doubleCount++;
                    return data.Data * 2;
                }
            );

        var valueStream = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-1",
            timestamp.AddSeconds(-1)
        );
        var doubleStream = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Double,
            "tenant-1",
            timestamp.AddSeconds(-1)
        );

        await PollR.TickAsync();

        await Assert.That(valueCount).IsEqualTo(1);
        await Assert.That(doubleCount).IsEqualTo(1);
        await Assert.That(await ReadDataAsync(valueStream)).IsEqualTo(1);
        await Assert.That(await ReadDataAsync(doubleStream)).IsEqualTo(2);
    }

    [Test]
    public async Task SubscribeProjection_CatchUpSubscriber_ReceivesProjectedCatchUp()
    {
        // Guards projected catch-up; registered subscribers move the next read cursor back.
        var currentTimestamp = DateTimeOffset.UtcNow;
        var catchUpTimestamp = currentTimestamp.AddSeconds(-30);
        var PollR = new PollRCaster<int, string>(
            (cursor, cancellationToken) =>
                ProduceRecordsAfterCursorAsync(
                    cursor,
                    cancellationToken,
                    new ProducerResult<int, string>(2, catchUpTimestamp, "tenant-1"),
                    new ProducerResult<int, string>(1, currentTimestamp, "tenant-1")
                )
        ).RegisterProjection(TestProjection.Value, data => data.Data * 10);

        await PollR.TickAsync();
        var stream = PollR.SubscribeProjection<TestProjection, int>(
            TestProjection.Value,
            "tenant-1",
            catchUpTimestamp.AddSeconds(-1)
        );
        await PollR.TickAsync();

        await Assert.That(await ReadDataAsync(stream)).IsEqualTo(20);
        await Assert.That(await ReadDataAsync(stream)).IsEqualTo(10);
    }

    [Test]
    public async Task TickAsync_BoundedChannelStreamFull_DropsNewWritesWithoutBlocking()
    {
        // Guards slow-consumer behavior; bounded streams drop new writes instead of growing indefinitely.
        var timestamp = DateTimeOffset.UtcNow;
        var PollR = new PollRCaster<int, string>(
            CreateProducer(
                new ProducerResult<int, string>(1, timestamp, "tenant-1"),
                new ProducerResult<int, string>(2, timestamp.AddTicks(1), "tenant-1")
            )
        );
        var stream = DefaultChannelDataStream<int, string>.CreateBoundedDropWrite(capacity: 1);

        PollR.Subscribe("tenant-1", timestamp.AddSeconds(-1), stream);

        await PollR.TickAsync();

        await Assert.That(stream.Reader.Count).IsEqualTo(1);

        var item = await stream.Reader.ReadAsync();

        await Assert.That(item.TryGetData(out var data)).IsTrue();
        await Assert.That(data.Data).IsEqualTo(1);
    }

    [Test]
    public async Task TickAsync_ConcurrentCalls_RunsProducerOneAtATime()
    {
        // Guards manual ticking; concurrent TickAsync callers cannot race shared cursor state.
        var producerCallCount = 0;
        var firstProducerEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var releaseFirstProducer = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var PollR = new PollRCaster<int, string>(ProduceRecordsAsync);

        var firstTick = PollR.TickAsync();
        await firstProducerEntered.Task;

        var secondTick = PollR.TickAsync();

        await Assert.That(producerCallCount).IsEqualTo(1);

        releaseFirstProducer.SetResult();
        await Task.WhenAll(firstTick, secondTick);

        await Assert.That(producerCallCount).IsEqualTo(2);

        async IAsyncEnumerable<ProducerResult<int, string>> ProduceRecordsAsync(
            DateTimeOffset cursor,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
                CancellationToken cancellationToken
        )
        {
            var callNumber = Interlocked.Increment(ref producerCallCount);

            if (callNumber == 1)
            {
                firstProducerEntered.SetResult();
                await releaseFirstProducer.Task.WaitAsync(cancellationToken);
            }

            await foreach (var result in ProduceRecordsAfterCursorAsync(cursor, cancellationToken))
            {
                yield return result;
            }
        }
    }

    [Test]
    public async Task StartAsync_CalledTwice_ReturnsSamePollingLoop()
    {
        // Guards lifecycle ownership; multiple StartAsync calls share one polling loop.
        var producerEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var PollR = new PollRCaster<int, string>(ProduceRecordsAsync);

        var firstStart = PollR.StartAsync();
        var secondStart = PollR.StartAsync();

        await producerEntered.Task;
        await PollR.StopAsync();

        await Assert.That(ReferenceEquals(firstStart, secondStart)).IsTrue();

        async IAsyncEnumerable<ProducerResult<int, string>> ProduceRecordsAsync(
            DateTimeOffset cursor,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
                CancellationToken cancellationToken
        )
        {
            producerEntered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);

            await foreach (var result in ProduceRecordsAfterCursorAsync(cursor, cancellationToken))
            {
                yield return result;
            }
        }
    }

    [Test]
    public async Task StopAsync_WhileStartAsyncIsRunning_BroadcastsCompletionOnce()
    {
        // Guards shutdown idempotency; StopAsync and the start loop do not emit duplicate terminal results.
        var producerEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var PollR = new PollRCaster<int, string>(ProduceRecordsAsync);
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-1", DateTimeOffset.UtcNow, stream);
        var startTask = PollR.StartAsync();

        await producerEntered.Task;
        await PollR.StopAsync();
        await startTask;

        await Assert.That(stream.StreamCompletedResults().Count).IsEqualTo(1);
        await Assert.That(stream.CompleteCount).IsEqualTo(1);

        async IAsyncEnumerable<ProducerResult<int, string>> ProduceRecordsAsync(
            DateTimeOffset cursor,
            [System.Runtime.CompilerServices.EnumeratorCancellation]
                CancellationToken cancellationToken
        )
        {
            producerEntered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);

            await foreach (var result in ProduceRecordsAfterCursorAsync(cursor, cancellationToken))
            {
                yield return result;
            }
        }
    }

    [Test]
    public async Task DisposeAsync_CalledTwice_CompletesSubscribersOnce()
    {
        // Guards cleanup idempotency; repeated disposal does not duplicate terminal messages.
        var PollR = new PollRCaster<int, string>(CreateProducer());
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        PollR.Subscribe("tenant-1", DateTimeOffset.UtcNow, stream);
        await PollR.TickAsync();

        await PollR.DisposeAsync();
        await PollR.DisposeAsync();

        await Assert.That(stream.StreamCompletedResults().Count).IsEqualTo(1);
        await Assert.That(stream.CompleteCount).IsEqualTo(1);
    }

    [Test]
    public async Task PollRBase_UsesSequenceCursor_DeliversCatchUpWithoutDateTimeCursor()
    {
        // Guards generic cursor support; the base poller works with deterministic sequence cursors.
        var PollR = new SequencePollRCaster<int, string>(
            (cursor, cancellationToken) =>
                ProduceSequenceRecordsAfterCursorAsync(
                    cursor,
                    cancellationToken,
                    new ProducerResult<int, string, long>(1, 1, "tenant-1"),
                    new ProducerResult<int, string, long>(2, 2, "tenant-1")
                )
        );
        var stream = new RecordingStream<IntervalData<int, string, long>>();

        PollR.Subscribe("tenant-1", 0, stream);

        await PollR.TickAsync();

        await Assert.That(stream.DataResults()).IsEquivalentTo([1, 2]);
    }

    [Test]
    public async Task TickAsync_CatchUpSubscriberJoinsBeforeNextRecord_DeliversReplayAndLiveRecordBySubscriberCursor()
    {
        // Guards mixed replay/live catch-up; the late subscriber gets replay plus the newly pulled record.
        var releasedCursor = 0L;
        var PollR = new SequencePollRCaster<int, string>(
            (cursor, cancellationToken) =>
            {
                releasedCursor++;

                return ProduceSequenceRecordsAfterCursorAsync(
                        cursor,
                        cancellationToken,
                        new ProducerResult<int, string, long>(1, 1, "tenant-1"),
                        new ProducerResult<int, string, long>(2, 2, "tenant-1")
                    )
                    .Where(result => result.Cursor <= releasedCursor);
            }
        );
        var firstStream = new RecordingStream<IntervalData<int, string, long>>();
        var secondStream = new RecordingStream<IntervalData<int, string, long>>();

        PollR.Subscribe("tenant-1", 0, firstStream);
        await PollR.TickAsync();

        await Assert.That(firstStream.DataResults()).IsEquivalentTo([1]);

        PollR.Subscribe("tenant-1", 0, secondStream);
        await PollR.TickAsync();

        await Assert.That(firstStream.DataResults()).IsEquivalentTo([1, 2]);
        await Assert.That(secondStream.DataResults()).IsEquivalentTo([1, 2]);
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

        foreach (var result in results.Where(result => result.Timestamp > cursor))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return result;
        }
    }

    static async IAsyncEnumerable<ProducerResult<int, string>> RecordCursorsAndProduceAsync(
        List<DateTimeOffset> cursors,
        DateTimeOffset cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken,
        params ProducerResult<int, string>[] results
    )
    {
        cursors.Add(cursor);

        await foreach (
            var result in ProduceRecordsAfterCursorAsync(cursor, cancellationToken, results)
        )
        {
            yield return result;
        }
    }

    static async IAsyncEnumerable<ProducerResult<int, string>> ThrowingProducer(
        DateTimeOffset cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        await Task.Yield();
        throw new InvalidOperationException("producer failed");
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
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
        Double,
    }

    static async IAsyncEnumerable<
        ProducerResult<int, string, long>
    > ProduceSequenceRecordsAfterCursorAsync(
        long cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation]
            CancellationToken cancellationToken,
        params ProducerResult<int, string, long>[] results
    )
    {
        await Task.Yield();

        foreach (var result in results.Where(result => result.Cursor > cursor))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return result;
        }
    }
}

sealed class SequencePollRCaster<TData, TPartition>(DataProducer<TData, TPartition, long> producer)
    : PollRBase<TData, TPartition, long>(
        producer,
        initialCursorFactory: () => 0,
        clampCursor: cursor => cursor
    )
    where TPartition : notnull;

sealed class RecordingStream<TData> : IDataStream<TData>
{
    readonly List<TData> _items = [];

    public IReadOnlyList<TData> Items => _items;

    public int CompleteCount { get; private set; }

    public ValueTask PushAsync(TData data, CancellationToken cancellationToken = default)
    {
        _items.Add(data);
        return ValueTask.CompletedTask;
    }

    public void Complete(CancellationToken cancellationToken = default)
    {
        CompleteCount++;
    }
}

static class IntervalDataTestExtensions
{
    public static IReadOnlyList<TData> DataResults<TData, TPartition, TCursor>(
        this RecordingStream<IntervalData<TData, TPartition, TCursor>> stream
    )
        where TPartition : notnull
        where TCursor : IComparable<TCursor> =>
        stream.Items.SelectMany(GetData<TData, TPartition, TCursor>).ToArray();

    public static IReadOnlyList<string> Errors<TData, TPartition, TCursor>(
        this RecordingStream<IntervalData<TData, TPartition, TCursor>> stream
    )
        where TPartition : notnull
        where TCursor : IComparable<TCursor> =>
        stream.Items.SelectMany(GetError<TData, TPartition, TCursor>).ToArray();

    public static IReadOnlyList<StreamCompletedResult> StreamCompletedResults<
        TData,
        TPartition,
        TCursor
    >(this RecordingStream<IntervalData<TData, TPartition, TCursor>> stream)
        where TPartition : notnull
        where TCursor : IComparable<TCursor> =>
        stream.Items.SelectMany(GetStreamCompletedResult<TData, TPartition, TCursor>).ToArray();

    static IEnumerable<TData> GetData<TData, TPartition, TCursor>(
        IntervalData<TData, TPartition, TCursor> item
    )
        where TPartition : notnull
        where TCursor : IComparable<TCursor>
    {
        if (item.TryGetData(out var result))
        {
            yield return result.Data;
        }
    }

    static IEnumerable<string> GetError<TData, TPartition, TCursor>(
        IntervalData<TData, TPartition, TCursor> item
    )
        where TPartition : notnull
        where TCursor : IComparable<TCursor>
    {
        if (item.TryGetError(out var result))
        {
            yield return result.ErrorMessage;
        }
    }

    static IEnumerable<StreamCompletedResult> GetStreamCompletedResult<TData, TPartition, TCursor>(
        IntervalData<TData, TPartition, TCursor> item
    )
        where TPartition : notnull
        where TCursor : IComparable<TCursor>
    {
        if (item.TryGetStreamCompleted(out var result))
        {
            yield return result;
        }
    }
}
