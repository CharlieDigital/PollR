using System.Threading.Channels;

namespace PollR;

public interface IDataStream<TData>
{
    // TODO(cleanup): Split the consumer read contract from the internal writer/completer
    // contract so subscribe overloads can expose an interface without leaking ChannelDataStream.
    ValueTask PushAsync(TData data, CancellationToken cancellationToken = default);

    void Complete(CancellationToken cancellationToken = default);
}

public class ChannelDataStream<TData>(Channel<TData> channel) : IDataStream<TData>
{
    public const int DefaultBoundedCapacity = 64;

    public const int MaxBoundedCapacity = 128;

    public ChannelReader<TData> Reader => channel.Reader;

    public static ChannelDataStream<TData> CreateUnbounded() =>
        new(Channel.CreateUnbounded<TData>());

    public static ChannelDataStream<TData> CreateBoundedDropWrite(int capacity) =>
        new(
            Channel.CreateBounded<TData>(
                new BoundedChannelOptions(ClampBoundedCapacity(capacity))
                {
                    FullMode = BoundedChannelFullMode.DropWrite,
                }
            )
        );

    public ValueTask PushAsync(TData data, CancellationToken cancellationToken = default) =>
        channel.Writer.WriteAsync(data, cancellationToken);

    public void Complete(CancellationToken cancellationToken = default) =>
        channel.Writer.TryComplete();

    protected static int ClampBoundedCapacity(int capacity) =>
        Math.Min(capacity, MaxBoundedCapacity);
}

public sealed class DefaultChannelDataStream<TData, TPartition>(
    Channel<IntervalData<TData, TPartition, DateTimeOffset>> channel
) : ChannelDataStream<IntervalData<TData, TPartition, DateTimeOffset>>(channel)
    where TPartition : notnull
{
    public static new DefaultChannelDataStream<TData, TPartition> CreateUnbounded() =>
        new(Channel.CreateUnbounded<IntervalData<TData, TPartition, DateTimeOffset>>());

    public static new DefaultChannelDataStream<TData, TPartition> CreateBoundedDropWrite(
        int capacity
    ) =>
        new(
            Channel.CreateBounded<IntervalData<TData, TPartition, DateTimeOffset>>(
                new BoundedChannelOptions(ClampBoundedCapacity(capacity))
                {
                    FullMode = BoundedChannelFullMode.DropWrite,
                }
            )
        );
}
