namespace PollR;

/// <summary>
/// Shared lifecycle contract for PollR runners.
/// </summary>
/// <remarks>
/// This stays intentionally small so both shared-feed and partitioned runners can
/// expose the same control surface without sharing scheduler internals.
/// </remarks>
public interface IPollRRunner
{
    Task StartAsync();

    Task StopAsync();

    Task TickAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Shared direct-subscription contract for PollR runners.
/// </summary>
public interface IPollRSubscriber<TData, TPartition, TCursor> : IPollRRunner
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    void Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        IDataStream<IntervalData<TData, TPartition, TCursor>> stream,
        CancellationToken cancellationToken = default
    );

    ChannelDataStream<IntervalData<TData, TPartition, TCursor>> Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        CancellationToken cancellationToken = default
    );

    ChannelDataStream<IntervalData<TData, TPartition, TCursor>> Subscribe(
        TPartition partition,
        TCursor cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Shared projection registration contract for public PollR runners.
/// </summary>
public interface IPollRProjectionRegistrar<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    void RegisterProjection<TPayload>(
        string key,
        Func<DataResult<TData, TPartition, TCursor>, TPayload> project
    );
}

/// <summary>
/// Shared projection subscription contract for public PollR runners.
/// </summary>
public interface IPollRProjectionSubscriber<TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    ChannelDataStream<IntervalData<TPayload, TPartition, TCursor>> SubscribeProjection<TPayload>(
        string key,
        TPartition partition,
        TCursor cursorPosition,
        CancellationToken cancellationToken = default
    );

    ChannelDataStream<IntervalData<TPayload, TPartition, TCursor>> SubscribeProjection<TPayload>(
        string key,
        TPartition partition,
        TCursor cursorPosition,
        int streamCapacity,
        CancellationToken cancellationToken = default
    );
}
