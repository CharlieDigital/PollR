namespace PollR;

/// <summary>
/// A default implementation of the abstract `PollRBase` class using `DateTimeOffset`
/// as the cursor type.
/// </summary>
/// <typeparam name="TData">The type of the data produced by the producer.</typeparam>
/// <typeparam name="TPartition">The type of the partition key.</typeparam>
/// <param name="producer">The data producer function.</param>
/// <param name="pollingInterval">(Optional) The interval at which to poll for new data.</param>
/// <param name="lookbackWindow">(Optional) The lookback window for data retrieval.</param>
/// <param name="maxLookbackWindow">(Optional) The maximum lookback window for data retrieval.</param>
/// <param name="cancellationToken">(Optional) The cancellation token to observe.</param>
public sealed class PollRCaster<TData, TPartition>(
    DataProducer<TData, TPartition> producer,
    TimeSpan? pollingInterval = null,
    TimeSpan? lookbackWindow = null,
    TimeSpan? maxLookbackWindow = null,
    CancellationToken cancellationToken = default
)
    : PollRBase<TData, TPartition, DateTimeOffset>(
        (cursorPosition, producerCancellationToken) =>
            ProduceAsync(producer, cursorPosition, producerCancellationToken),
        () => DateTimeOffset.UtcNow - (lookbackWindow ?? DefaultLookbackWindow),
        cursorPosition => ClampCursorPosition(cursorPosition, maxLookbackWindow),
        pollingInterval,
        cancellationToken
    )
    where TPartition : notnull
{
    static readonly TimeSpan DefaultLookbackWindow = TimeSpan.FromMinutes(1);
    static readonly TimeSpan DefaultMaxLookbackWindow = TimeSpan.FromMinutes(5);

    // Projection registration is the shared, low-allocation path: register stable shapes
    // at poller setup, then subscribers attach by key. Endpoint-level ad-hoc projections
    // can still be layered outside core when a shape is request-specific, but those cannot
    // amortize projection or serialization across subscribers.

    /// <summary>
    /// Registers a shared typed projection.
    /// </summary>
    /// <remarks>
    /// Registered projections are keyed at poller setup and shared by subscribers that
    /// attach with the same key. When a produced record has matching subscribers, the
    /// projection runs once for that key and the projected payload is fanned out to those
    /// subscribers.
    ///
    /// Use this for stable shapes that multiple endpoints or clients can share. For
    /// request-specific shapes, use the ASP.NET ad-hoc projection APIs instead; those are
    /// more flexible, but projection work cannot be amortized across subscribers.
    /// </remarks>
    public PollRCaster<TData, TPartition> RegisterProjection<TPayload>(
        string key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> project
    )
    {
        RegisterProjectionCore(key, project);
        return this;
    }

    /// <summary>
    /// Registers a shared typed projection using an enum key.
    /// </summary>
    /// <remarks>
    /// Enum keys avoid stringly typed call sites while still producing a stable full-name
    /// key internally.
    /// </remarks>
    public PollRCaster<TData, TPartition> RegisterProjection<TEnum, TPayload>(
        TEnum key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> project
    )
        where TEnum : struct, Enum => RegisterProjection(GetProjectionKey(key), project);

    /// <summary>
    /// Registers a shared serialized projection.
    /// </summary>
    /// <remarks>
    /// This is the lowest-cost HTTP/SSE path when many subscribers receive the same shape.
    /// The delegate returns the final serialized string, so serialization happens once per
    /// produced record per projection key, not once per connected subscriber.
    ///
    /// Prefer this when the serializer can be chosen at poller setup, including
    /// source-generated serializers for maximum throughput. Use ad-hoc serialized
    /// projections only when the serialized shape is endpoint-specific or request-specific.
    /// </remarks>
    public PollRCaster<TData, TPartition> RegisterSerializedProjection(
        string key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, string> serialize
    ) => RegisterProjection(key, serialize);

    /// <summary>
    /// Registers a shared serialized projection using an enum key.
    /// </summary>
    /// <remarks>
    /// Enum keys avoid stringly typed call sites while preserving the shared serialization
    /// behavior of registered serialized projections.
    /// </remarks>
    public PollRCaster<TData, TPartition> RegisterSerializedProjection<TEnum>(
        TEnum key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, string> serialize
    )
        where TEnum : struct, Enum =>
        RegisterSerializedProjection(GetProjectionKey(key), serialize);

    /// <summary>
    /// Registers a shared typed projection plus serializer.
    /// </summary>
    /// <remarks>
    /// This overload keeps payload shaping separate from string serialization while
    /// preserving registered fan-out behavior. Both projection and serialization are paid
    /// once per produced record per key, then the serialized string is shared by matching
    /// subscribers.
    /// </remarks>
    public PollRCaster<TData, TPartition> RegisterSerializedProjection<TPayload>(
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
    public PollRCaster<TData, TPartition> RegisterSerializedProjection<TEnum, TPayload>(
        TEnum key,
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> project,
        Func<TPayload, string> serialize
    )
        where TEnum : struct, Enum =>
        RegisterSerializedProjection(GetProjectionKey(key), project, serialize);

    /// <summary>
    /// Subscribes to a shared typed projection.
    /// </summary>
    /// <remarks>
    /// Subscribers using the same projection key share the same projected payload for a
    /// produced record. This keeps per-subscriber work limited to stream writes once the
    /// record has been projected.
    /// </remarks>
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
    /// Subscribes to a shared typed projection with explicit stream capacity.
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
    /// <remarks>
    /// This is intended for pass-through transports such as SSE where the payload is already
    /// a string. Matching subscribers receive the same serialized payload without invoking a
    /// serializer for each connection.
    /// </remarks>
    public ChannelDataStream<
        IntervalData<string, TPartition, DateTimeOffset>
    > SubscribeSerializedProjection(
        string key,
        TPartition partition,
        DateTimeOffset cursorPosition,
        CancellationToken cancellationToken = default
    ) => SubscribeProjection<string>(key, partition, cursorPosition, cancellationToken);

    /// <summary>
    /// Subscribes to a shared serialized projection with explicit stream capacity.
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

    static async IAsyncEnumerable<ProducerResult<TData, TPartition, DateTimeOffset>> ProduceAsync(
        DataProducer<TData, TPartition> producer,
        DateTimeOffset cursorPosition,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        await foreach (var item in producer(cursorPosition, cancellationToken))
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
