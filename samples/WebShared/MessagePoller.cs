using Microsoft.EntityFrameworkCore;

namespace PollR.Samples.WebShared;

/// <summary>
/// The poller that runs inside of the hosted service and encapsulates the producer.
/// </summary>
public sealed class MessagePoller
{
    readonly IDbContextFactory<MessageDb> _dbFactory;

    public MessagePoller(IDbContextFactory<MessageDb> dbFactory)
    {
        _dbFactory = dbFactory;
        Poller = new(ProduceAsync);
    }

    public PollRCaster<MessageEvent, string> Poller { get; }

    public PollRCaster<MessageEvent, string> RegisterSerializedProjection(
        Func<DataResult<MessageEvent, string, DateTimeOffset>, string> serialize
    )
    {
        Poller.RegisterSerializedProjection(MessageProjection.Full, serialize);
        return Poller;
    }

    /// <summary>
    /// The simple producer implementation that reads from an in-memory EF database as an example.
    /// </summary>
    async IAsyncEnumerable<ProducerResult<MessageEvent, string>> ProduceAsync(
        DateTimeOffset cursor,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        await using var db = await _dbFactory.CreateDbContextAsync(cancellationToken);

        // PollR asks for records newer than the cursor it is currently reading from.
        await foreach (
            var message in db
                .Messages.AsNoTracking()
                .Where(message => message.CreatedAt > cursor)
                .OrderBy(message => message.CreatedAt)
                .AsAsyncEnumerable()
                .WithCancellation(cancellationToken)
        )
        {
            // Each result includes data, cursor, and partition so PollR can fan out correctly.
            yield return new ProducerResult<MessageEvent, string>(
                new MessageEvent(message.Id, message.Topic, message.Text, message.CreatedAt),
                message.CreatedAt,
                message.Topic
            );
        }
    }
}
