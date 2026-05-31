using System.Collections.Concurrent;
using ZLinq;

namespace PollR;

internal sealed class Subscriber<TData, TPartition, TCursor>(
    TCursor cursorPosition,
    IDataStream<IntervalData<TData, TPartition, TCursor>> stream
)
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    // Cursor mutation is intentionally lock-free because core delivery and catch-up promotion
    // run inside PollRBase's serialized tick loop. Add synchronization here if cursor
    // advancement is ever exposed to external relay threads or multiple concurrent fan-out loops.
    TCursor _cursorPosition = cursorPosition;
    int _completed;

    public IDataStream<IntervalData<TData, TPartition, TCursor>> Stream { get; } = stream;

    public CancellationTokenRegistration CancellationRegistration { get; set; }

    public TCursor CursorPosition => _cursorPosition;

    public bool IsCompleted => Volatile.Read(ref _completed) == 1;

    public bool TryAdvanceCursor(TCursor cursorPosition)
    {
        if (cursorPosition.CompareTo(_cursorPosition) <= 0)
        {
            return false;
        }

        _cursorPosition = cursorPosition;
        return true;
    }

    public void CatchUpTo(TCursor cursorPosition)
    {
        if (cursorPosition.CompareTo(_cursorPosition) > 0)
        {
            _cursorPosition = cursorPosition;
        }
    }

    public void Complete(bool disposeCancellationRegistration)
    {
        if (Interlocked.Exchange(ref _completed, 1) == 1)
        {
            return;
        }

        if (disposeCancellationRegistration)
        {
            CancellationRegistration.Dispose();
        }

        Stream.Complete();
    }
}

internal sealed class PartitionSubscribers<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    readonly ConcurrentDictionary<long, Subscriber<TData, TPartition, TCursor>> _catchingUp = [];
    readonly ConcurrentDictionary<long, Subscriber<TData, TPartition, TCursor>> _current = [];

    public IEnumerable<Subscriber<TData, TPartition, TCursor>> All =>
        _catchingUp.IsEmpty ? _current.Values : EnumerateAll();

    public bool IsEmpty => _catchingUp.IsEmpty && _current.IsEmpty;

    public void Add(
        long subscriberId,
        Subscriber<TData, TPartition, TCursor> subscriber,
        bool isCatchingUp
    )
    {
        var subscribers = isCatchingUp ? _catchingUp : _current;
        subscribers[subscriberId] = subscriber;
    }

    public bool TryRemove(long subscriberId, out Subscriber<TData, TPartition, TCursor>? subscriber)
    {
        var removedCatchingUp = _catchingUp.TryRemove(subscriberId, out var catchingUpSubscriber);
        var removedCurrent = _current.TryRemove(subscriberId, out var currentSubscriber);

        subscriber = catchingUpSubscriber ?? currentSubscriber;
        return removedCatchingUp || removedCurrent;
    }

    public void PromoteCaughtUp(TCursor cursorPosition)
    {
        foreach (var (subscriberId, subscriber) in _catchingUp.AsValueEnumerable())
        {
            subscriber.CatchUpTo(cursorPosition);

            _current[subscriberId] = subscriber;

            if (_catchingUp.TryRemove(subscriberId, out var removedSubscriber))
            {
                if (removedSubscriber.IsCompleted)
                {
                    _current.TryRemove(subscriberId, out _);
                }
            }
            else if (subscriber.IsCompleted)
            {
                _current.TryRemove(subscriberId, out _);
            }
        }
    }

    IEnumerable<Subscriber<TData, TPartition, TCursor>> EnumerateAll()
    {
        foreach (var subscriber in _catchingUp.Values)
        {
            yield return subscriber;
        }

        foreach (var subscriber in _current.Values)
        {
            yield return subscriber;
        }
    }
}

internal sealed record PendingSubscriber<TData, TPartition, TCursor>(
    long SubscriberId,
    TPartition Partition,
    Subscriber<TData, TPartition, TCursor> Subscriber
)
    where TPartition : notnull
    where TCursor : IComparable<TCursor>;

public readonly struct IntervalData<TData, TPartition, TCursor>
    where TPartition : notnull
    where TCursor : IComparable<TCursor>
{
    const byte ErrorKind = 1;
    const byte StreamCompletedKind = 2;
    const byte DataKind = 3;

    readonly byte _kind;
    readonly ErrorResult _error;
    readonly StreamCompletedResult _streamCompleted;
    readonly DataResult<TData, TPartition, TCursor> _data;

    IntervalData(ErrorResult error)
    {
        _kind = ErrorKind;
        _error = error;
        _streamCompleted = default;
        _data = default;
    }

    IntervalData(StreamCompletedResult streamCompleted)
    {
        _kind = StreamCompletedKind;
        _error = default;
        _streamCompleted = streamCompleted;
        _data = default;
    }

    IntervalData(DataResult<TData, TPartition, TCursor> data)
    {
        _kind = DataKind;
        _error = default;
        _streamCompleted = default;
        _data = data;
    }

    public static implicit operator IntervalData<TData, TPartition, TCursor>(ErrorResult error) =>
        new(error);

    public static implicit operator IntervalData<TData, TPartition, TCursor>(
        StreamCompletedResult streamCompleted
    ) => new(streamCompleted);

    public static implicit operator IntervalData<TData, TPartition, TCursor>(
        DataResult<TData, TPartition, TCursor> data
    ) => new(data);

    public bool TryGetData(out DataResult<TData, TPartition, TCursor> data)
    {
        data = _data;
        return _kind == DataKind;
    }

    public bool TryGetError(out ErrorResult error)
    {
        error = _error;
        return _kind == ErrorKind;
    }

    public bool TryGetStreamCompleted(out StreamCompletedResult streamCompleted)
    {
        streamCompleted = _streamCompleted;
        return _kind == StreamCompletedKind;
    }
}

public record struct NoDataResult();

public record struct ErrorResult(string ErrorMessage);

public record struct StreamCompletedResult();

public record struct DataResult<TData, TPartition, TCursor>(
    TData Data,
    TCursor Cursor,
    TPartition Partition
)
    where TCursor : IComparable<TCursor>;

public record struct ProducerResult<TData, TPartition, TCursor>(
    TData Data,
    TCursor Cursor,
    TPartition Partition
)
    where TCursor : IComparable<TCursor>;

public record struct ProducerResult<TData, TPartition>(
    TData Data,
    DateTimeOffset Cursor,
    TPartition Partition
)
{
    public DateTimeOffset Timestamp => Cursor;
}
