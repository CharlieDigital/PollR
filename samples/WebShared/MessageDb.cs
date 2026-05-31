using Microsoft.EntityFrameworkCore;

namespace PollR.Samples.WebShared;

public sealed class Message
{
    public long Id { get; set; }

    public required string Topic { get; set; }

    public required string Text { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>
/// In memory EF database for the sample.
/// </summary>
public sealed class MessageDb(DbContextOptions<MessageDb> options) : DbContext(options)
{
    public DbSet<Message> Messages => Set<Message>();
}
