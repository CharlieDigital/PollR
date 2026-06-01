namespace PollR;

/// <summary>
/// DateTimeOffset convenience surface for the partitioned poller.
/// </summary>
/// <remarks>
/// This mirrors the existing PollRCaster public surface while changing the polling
/// unit from one shared feed to one active partition feed.
/// </remarks>
public sealed class PartitionedPollRCaster<TData, TPartition>(
    PartitionDataProducer<TData, TPartition> producer,
    TimeSpan? pollingInterval = null,
    TimeSpan? lookbackWindow = null,
    TimeSpan? maxLookbackWindow = null,
    int? maxConcurrentPartitions = null,
    CancellationToken cancellationToken = default
)
    : PartitionedPollRBase<TData, TPartition, DateTimeOffset>(
        (partition, cursorPosition, producerCancellationToken) =>
            ProduceAsync(producer, partition, cursorPosition, producerCancellationToken),
        () => DateTimeOffset.UtcNow - (lookbackWindow ?? DefaultLookbackWindow),
        cursorPosition => ClampCursorPosition(cursorPosition, maxLookbackWindow),
        pollingInterval,
        maxConcurrentPartitions,
        cancellationToken
    ),
        IPollRProjectionRegistrar<TData, TPartition, DateTimeOffset>,
        IPollRProjectionSubscriber<TPartition, DateTimeOffset>
    where TPartition : notnull
{
    static readonly TimeSpan DefaultLookbackWindow = TimeSpan.FromMinutes(1);
    static readonly TimeSpan DefaultMaxLookbackWindow = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Registers a shared typed projection.
    /// </summary>
    public PartitionedPollRCaster<TData, TPartition> RegisterProjection<TPayload>(
        string key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> project
    )
    {
        RegisterProjectionCore(key, project);
        return this;
    }

    void IPollRProjectionRegistrar<TData, TPartition, DateTimeOffset>.RegisterProjection<TPayload>(
        string key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> project
    ) => RegisterProjection(key, project);

    /// <summary>
    /// Registers a shared typed projection using an enum key.
    /// </summary>
    public PartitionedPollRCaster<TData, TPartition> RegisterProjection<TEnum, TPayload>(
        TEnum key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> project
    )
        where TEnum : struct, Enum => RegisterProjection(GetProjectionKey(key), project);

    /// <summary>
    /// Registers a shared serialized projection.
    /// </summary>
    public PartitionedPollRCaster<TData, TPartition> RegisterSerializedProjection(
        string key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, string> serialize
    ) => RegisterProjection(key, serialize);

    /// <summary>
    /// Registers a shared serialized projection using an enum key.
    /// </summary>
    public PartitionedPollRCaster<TData, TPartition> RegisterSerializedProjection<TEnum>(
        TEnum key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, string> serialize
    )
        where TEnum : struct, Enum =>
        RegisterSerializedProjection(GetProjectionKey(key), serialize);

    /// <summary>
    /// Registers a shared typed projection plus serializer.
    /// </summary>
    public PartitionedPollRCaster<TData, TPartition> RegisterSerializedProjection<TPayload>(
        string key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> project,
        Func<TPayload, string> serialize
    )
    {
        ArgumentNullException.ThrowIfNull(project);
        ArgumentNullException.ThrowIfNull(serialize);

        return RegisterSerializedProjection(key, data => serialize(project(data)));
    }

    /// <summary>
    /// Registers a shared typed projection plus serializer using an enum key.
    /// </summary>
    public PartitionedPollRCaster<TData, TPartition> RegisterSerializedProjection<TEnum, TPayload>(
        TEnum key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> project,
        Func<TPayload, string> serialize
    )
        where TEnum : struct, Enum
    {
        return RegisterSerializedProjection(GetProjectionKey(key), project, serialize);
    }

    /// <summary>
    /// Convenience default-channel subscribe overload.
    /// </summary>
    public new DefaultChannelDataStream<TData, TPartition> Subscribe(
        TPartition partition,
        DateTimeOffset cursorPosition,
        CancellationToken cancellationToken = default
    ) =>
        Subscribe(
            partition,
            cursorPosition,
            DefaultChannelDataStream<TData, TPartition>.DefaultBoundedCapacity,
            cancellationToken
        );

    /// <summary>
    /// Convenience default-channel subscribe overload with explicit capacity.
    /// </summary>
    public new DefaultChannelDataStream<TData, TPartition> Subscribe(
        TPartition partition,
        DateTimeOffset cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    )
    {
        var stream = DefaultChannelDataStream<TData, TPartition>.CreateBoundedDropWrite(
            streamCapacity
        );
        Subscribe(partition, cursorPosition, stream, cancellationToken);
        return stream;
    }

    /// <summary>
    /// Subscribes to a shared typed projection.
    /// </summary>
    public ChannelDataStream<
        IntervalData<TPayload, TPartition, DateTimeOffset>
    > SubscribeProjection<TPayload>(
        string key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        CancellationToken cancellationToken = default
    ) =>
        SubscribeProjection<TPayload>(
            key,
            partition,
            cursorPosition,
            ChannelDataStream<
                IntervalData<TPayload, TPartition, DateTimeOffset>
            >.DefaultBoundedCapacity,
            cancellationToken
        );

    /// <summary>
    /// Subscribes to a shared typed projection with explicit capacity.
    /// </summary>
    public ChannelDataStream<
        IntervalData<TPayload, TPartition, DateTimeOffset>
    > SubscribeProjection<TPayload>(
        string key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    ) =>
        SubscribeProjectionCore<TPayload>(
            key,
            partition,
            cursorPosition,
            streamCapacity,
            cancellationToken
        );

    /// <summary>
    /// Subscribes to a shared typed projection using an enum key.
    /// </summary>
    public ChannelDataStream<
        IntervalData<TPayload, TPartition, DateTimeOffset>
    > SubscribeProjection<TEnum, TPayload>(
        TEnum key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        CancellationToken cancellationToken = default
    )
        where TEnum : struct, Enum =>
        SubscribeProjection<TPayload>(
            GetProjectionKey(key),
            partition,
            cursorPosition,
            cancellationToken
        );

    /// <summary>
    /// Subscribes to a shared typed projection using an enum key and explicit capacity.
    /// </summary>
    public ChannelDataStream<
        IntervalData<TPayload, TPartition, DateTimeOffset>
    > SubscribeProjection<TEnum, TPayload>(
        TEnum key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    )
        where TEnum : struct, Enum =>
        SubscribeProjection<TPayload>(
            GetProjectionKey(key),
            partition,
            cursorPosition,
            streamCapacity,
            cancellationToken
        );

    /// <summary>
    /// Subscribes to a shared serialized projection.
    /// </summary>
    public ChannelDataStream<
        IntervalData<string, TPartition, DateTimeOffset>
    > SubscribeSerializedProjection(
        string key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        CancellationToken cancellationToken = default
    ) => SubscribeProjection<string>(key, partition, cursorPosition, cancellationToken);

    /// <summary>
    /// Subscribes to a shared serialized projection with explicit capacity.
    /// </summary>
    public ChannelDataStream<
        IntervalData<string, TPartition, DateTimeOffset>
    > SubscribeSerializedProjection(
        string key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    ) =>
        SubscribeProjection<string>(
            key,
            partition,
            cursorPosition,
            streamCapacity,
            cancellationToken
        );

    /// <summary>
    /// Subscribes to a shared serialized projection using an enum key.
    /// </summary>
    public ChannelDataStream<
        IntervalData<string, TPartition, DateTimeOffset>
    > SubscribeSerializedProjection<TEnum>(
        TEnum key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        CancellationToken cancellationToken = default
    )
        where TEnum : struct, Enum =>
        SubscribeSerializedProjection(
            GetProjectionKey(key),
            partition,
            cursorPosition,
            cancellationToken
        );

    /// <summary>
    /// Subscribes to a shared serialized projection using an enum key and explicit capacity.
    /// </summary>
    public ChannelDataStream<
        IntervalData<string, TPartition, DateTimeOffset>
    > SubscribeSerializedProjection<TEnum>(
        TEnum key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    )
        where TEnum : struct, Enum =>
        SubscribeSerializedProjection(
            GetProjectionKey(key),
            partition,
            cursorPosition,
            streamCapacity,
            cancellationToken
        );

    static string GetProjectionKey<TEnum>(TEnum key)
        where TEnum : struct, Enum => $"{typeof(TEnum).FullName ?? typeof(TEnum).Name}.{key}";

    static async IAsyncEnumerable<ProducerResult<TData, TPartition, DateTimeOffset>> ProduceAsync(
        PartitionDataProducer<TData, TPartition> producer,
        TPartition partition,
        DateTimeOffset cursorPosition,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        await foreach (var item in producer(partition, cursorPosition, cancellationToken))
        {
            yield return new ProducerResult<TData, TPartition, DateTimeOffset>(
                item.Data,
                item.Cursor,
                item.Partition
            );
        }
    }

    static DateTimeOffset ClampCursorPosition(
        DateTimeOffset cursorPosition,
        TimeSpan? maxLookbackWindow
    )
    {
        var oldestAllowedCursor =
            DateTimeOffset.UtcNow - (maxLookbackWindow ?? DefaultMaxLookbackWindow);
        return cursorPosition < oldestAllowedCursor ? oldestAllowedCursor : cursorPosition;
    }
}
