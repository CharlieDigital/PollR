namespace PollR.Tests;

public class PollRContextTests
{
    [Test]
    public async Task Subscribe_StagesJoinUntilRegistration_DoesNotExposeSubscriber()
    {
        // Guards that Subscribe only stages joins; the subscriber becomes visible at the next tick boundary.
        var context = CreateContext<int, string>();
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();

        context.Subscribe("tenant-1", DateTimeOffset.UtcNow, stream, CancellationToken.None);

        await Assert.That(context.TryGetPartitionSubscribers("tenant-1", out _)).IsFalse();

        context.RegisterPendingSubscribers();

        await Assert
            .That(context.TryGetPartitionSubscribers("tenant-1", out var subscribers))
            .IsTrue();
        await Assert.That(subscribers!.All.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task Subscribe_RegistersOlderCursor_UsesCatchUpCursorOnNextTick()
    {
        // Guards catch-up behavior; an older joining cursor drives the next poll cursor after registration.
        var context = CreateContext<int, string>();
        var currentCursor = DateTimeOffset.UtcNow.AddSeconds(-10);
        var catchUpCursor = currentCursor.AddMinutes(-1);

        context.CompletePollingTick(currentCursor);
        context.Subscribe(
            "tenant-1",
            catchUpCursor,
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>(),
            CancellationToken.None
        );
        context.RegisterPendingSubscribers();

        await Assert.That(context.GetNextCursorPosition()).IsEqualTo(catchUpCursor);
    }

    [Test]
    public async Task CompletePollingTick_CompletesCatchUp_PromotesSubscriberAndReturnsToCurrentCursor()
    {
        // Guards promotion; after a catch-up tick completes, the next read resumes from the stream cursor.
        var context = CreateContext<int, string>();
        var currentCursor = DateTimeOffset.UtcNow.AddSeconds(-10);
        var catchUpCursor = currentCursor.AddMinutes(-1);
        var completedCursor = DateTimeOffset.UtcNow;

        context.CompletePollingTick(currentCursor);
        context.Subscribe(
            "tenant-1",
            catchUpCursor,
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>(),
            CancellationToken.None
        );
        context.RegisterPendingSubscribers();
        context.CompletePollingTick(completedCursor);

        await Assert.That(context.GetNextCursorPosition()).IsEqualTo(completedCursor);
    }

    [Test]
    public async Task Subscribe_RequestsCursorOlderThanMaxLookback_ClampsNextCursor()
    {
        // Guards bounded replay; requested catch-up cannot move the poll cursor past max lookback.
        var maxLookbackWindow = TimeSpan.FromMinutes(5);
        var context = CreateContext<int, string>(maxLookbackWindow: maxLookbackWindow);
        var oldestAllowedBeforeSubscribe = DateTimeOffset.UtcNow - maxLookbackWindow;

        context.Subscribe(
            "tenant-1",
            DateTimeOffset.UtcNow.AddHours(-1),
            new RecordingStream<IntervalData<int, string, DateTimeOffset>>(),
            CancellationToken.None
        );
        context.RegisterPendingSubscribers();

        var nextCursor = context.GetNextCursorPosition();

        await Assert.That(nextCursor >= oldestAllowedBeforeSubscribe).IsTrue();
        await Assert.That(nextCursor <= DateTimeOffset.UtcNow - maxLookbackWindow).IsTrue();
    }

    [Test]
    public async Task SubscriberCancellation_CancelsBeforeRegistration_CompletesStreamAndSkipsRegistration()
    {
        // Guards disconnect cleanup; a pending subscriber can cancel before the next tick without registering.
        var context = CreateContext<int, string>();
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();
        using var cancellationTokenSource = new CancellationTokenSource();

        context.Subscribe("tenant-1", DateTimeOffset.UtcNow, stream, cancellationTokenSource.Token);
        await cancellationTokenSource.CancelAsync();

        context.RegisterPendingSubscribers();

        await Assert.That(stream.IsCompleted).IsTrue();
        await Assert.That(context.TryGetPartitionSubscribers("tenant-1", out _)).IsFalse();
    }

    [Test]
    public async Task SubscriberCancellation_CancelsAfterRegistration_RemovesSubscriber()
    {
        // Guards active disconnect cleanup; canceling a registered subscriber removes the empty partition bucket.
        var context = CreateContext<int, string>();
        var stream = new RecordingStream<IntervalData<int, string, DateTimeOffset>>();
        using var cancellationTokenSource = new CancellationTokenSource();

        context.Subscribe("tenant-1", DateTimeOffset.UtcNow, stream, cancellationTokenSource.Token);
        context.RegisterPendingSubscribers();
        await cancellationTokenSource.CancelAsync();

        await Assert.That(stream.IsCompleted).IsTrue();
        await Assert.That(context.TryGetPartitionSubscribers("tenant-1", out _)).IsFalse();
    }

    static PollRContext<TData, TPartition, DateTimeOffset> CreateContext<TData, TPartition>(
        TimeSpan? pollingInterval = null,
        TimeSpan? lookbackWindow = null,
        TimeSpan? maxLookbackWindow = null
    )
        where TPartition : notnull =>
        new(
            pollingInterval,
            () => DateTimeOffset.UtcNow - (lookbackWindow ?? TimeSpan.FromMinutes(1)),
            cursor =>
            {
                var oldestAllowedCursor =
                    DateTimeOffset.UtcNow - (maxLookbackWindow ?? TimeSpan.FromMinutes(5));
                return cursor < oldestAllowedCursor ? oldestAllowedCursor : cursor;
            },
            new CancellationTokenSource()
        );

    sealed class RecordingStream<TData> : IDataStream<TData>
    {
        readonly List<TData> _items = [];

        public IReadOnlyList<TData> Items => _items;

        public bool IsCompleted { get; private set; }

        public ValueTask PushAsync(TData data, CancellationToken cancellationToken = default)
        {
            _items.Add(data);
            return ValueTask.CompletedTask;
        }

        public void Complete(CancellationToken cancellationToken = default)
        {
            IsCompleted = true;
        }
    }
}
