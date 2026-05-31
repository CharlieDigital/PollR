namespace PollR.Samples.WebShared;

public sealed record WriteMessage(string Text);

public sealed record MessageEvent(long Id, string Topic, string Text, DateTimeOffset CreatedAt);

public enum MessageProjection
{
    Full,
}
