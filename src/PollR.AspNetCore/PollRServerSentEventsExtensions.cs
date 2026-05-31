using System.Globalization;
using System.Net.ServerSentEvents;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;

namespace PollR.AspNetCore;

public static class PollRServerSentEventsExtensions
{
    public static PollRHttpBuilder<TData, TPartition> ForHttp<TData, TPartition>(
        this PollRCaster<TData, TPartition> poller,
        IHttpContextAccessor httpContextAccessor
    )
        where TPartition : notnull => new(poller, httpContextAccessor);
}

public readonly record struct PollRServerSentEventsOptions(
    TimeSpan ReconnectionInterval,
    int StreamCapacity
)
{
    public static PollRServerSentEventsOptions Default { get; } =
        new(
            TimeSpan.FromSeconds(3),
            DefaultChannelDataStream<object, string>.DefaultBoundedCapacity
        );
}

public readonly record struct PollRHttpBuilder<TData, TPartition>(
    PollRCaster<TData, TPartition> Poller,
    IHttpContextAccessor HttpContextAccessor
)
    where TPartition : notnull
{
    public PollRSubscriptionBuilder<TData, TPartition> WithSubscription(
        TPartition partition,
        DateTimeOffset defaultCursor
    ) => new(Poller, HttpContextAccessor, partition, defaultCursor);
}

public readonly record struct PollRSubscriptionBuilder<TData, TPartition>(
    PollRCaster<TData, TPartition> Poller,
    IHttpContextAccessor HttpContextAccessor,
    TPartition Partition,
    DateTimeOffset DefaultCursor,
    PollRServerSentEventsOptions Options,
    Func<TPartition, string?> EventTypeSelector
)
    where TPartition : notnull
{
    public PollRSubscriptionBuilder(
        PollRCaster<TData, TPartition> poller,
        IHttpContextAccessor httpContextAccessor,
        TPartition partition,
        DateTimeOffset defaultCursor
    )
        : this(
            poller,
            httpContextAccessor,
            partition,
            defaultCursor,
            PollRServerSentEventsOptions.Default,
            _ => null
        )
    { }

    public PollRSubscriptionBuilder<TData, TPartition> WithOptions(
        PollRServerSentEventsOptions options
    ) => this with { Options = options };

    public PollRSubscriptionBuilder<TData, TPartition> WithOptions(
        TimeSpan? reconnectionInterval = null,
        int? streamCapacity = null
    ) =>
        this with
        {
            Options = new PollRServerSentEventsOptions(
                reconnectionInterval ?? Options.ReconnectionInterval,
                streamCapacity ?? Options.StreamCapacity
            ),
        };

    public PollRSubscriptionBuilder<TData, TPartition> WithSseEventType(
        Func<TPartition, string?> eventTypeSelector
    ) => this with { EventTypeSelector = eventTypeSelector };

    public ServerSentEventsResult<TPayload> WithProjection<TPayload>(
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> map
    ) => WithAdHocProjection(map);

    public ServerSentEventsResult<TPayload> WithAdHocProjection<TPayload>(
        Func<DataResult<TData, TPartition, DateTimeOffset>, TPayload> map
    )
    {
        var httpContext = GetHttpContext();
        var stream = Poller.Subscribe(
            Partition,
            GetCursor(httpContext),
            Options.StreamCapacity,
            httpContext.RequestAborted
        );

        return TypedResults.ServerSentEvents(
            ReadServerSentEventsAsync(
                stream.Reader,
                map,
                EventTypeSelector,
                Options.ReconnectionInterval,
                httpContext.RequestAborted
            )
        );
    }

    public ServerSentEventsResult<string> WithAdHocSerializedProjection(
        Func<DataResult<TData, TPartition, DateTimeOffset>, string> serialize
    ) => WithAdHocProjection(serialize);

    public ServerSentEventsResult<string> WithRegisteredSerializedProjection(string key)
    {
        var httpContext = GetHttpContext();
        var stream = Poller.SubscribeSerializedProjection(
            key,
            Partition,
            GetCursor(httpContext),
            Options.StreamCapacity,
            httpContext.RequestAborted
        );

        return TypedResults.ServerSentEvents(
            ReadServerSentEventsAsync(
                stream.Reader,
                data => data.Data,
                EventTypeSelector,
                Options.ReconnectionInterval,
                httpContext.RequestAborted
            )
        );
    }

    public ServerSentEventsResult<string> WithRegisteredSerializedProjection<TEnum>(TEnum key)
        where TEnum : struct, Enum
    {
        var httpContext = GetHttpContext();
        var stream = Poller.SubscribeSerializedProjection(
            key,
            Partition,
            GetCursor(httpContext),
            Options.StreamCapacity,
            httpContext.RequestAborted
        );

        return TypedResults.ServerSentEvents(
            ReadServerSentEventsAsync(
                stream.Reader,
                data => data.Data,
                EventTypeSelector,
                Options.ReconnectionInterval,
                httpContext.RequestAborted
            )
        );
    }

    public ServerSentEventsResult<TPayload> WithRegisteredProjection<TPayload>(string key)
    {
        var httpContext = GetHttpContext();
        var stream = Poller.SubscribeProjection<TPayload>(
            key,
            Partition,
            GetCursor(httpContext),
            Options.StreamCapacity,
            httpContext.RequestAborted
        );

        return TypedResults.ServerSentEvents(
            ReadServerSentEventsAsync(
                stream.Reader,
                data => data.Data,
                EventTypeSelector,
                Options.ReconnectionInterval,
                httpContext.RequestAborted
            )
        );
    }

    public ServerSentEventsResult<TPayload> WithRegisteredProjection<TEnum, TPayload>(TEnum key)
        where TEnum : struct, Enum
    {
        var httpContext = GetHttpContext();
        var stream = Poller.SubscribeProjection<TEnum, TPayload>(
            key,
            Partition,
            GetCursor(httpContext),
            Options.StreamCapacity,
            httpContext.RequestAborted
        );

        return TypedResults.ServerSentEvents(
            ReadServerSentEventsAsync(
                stream.Reader,
                data => data.Data,
                EventTypeSelector,
                Options.ReconnectionInterval,
                httpContext.RequestAborted
            )
        );
    }

    HttpContext GetHttpContext() =>
        HttpContextAccessor.HttpContext
        ?? throw new InvalidOperationException("The current HTTP context is not available.");

    DateTimeOffset GetCursor(HttpContext httpContext)
    {
        if (
            httpContext.Request.Headers.TryGetValue("Last-Event-ID", out var lastEventId)
            && DateTimeOffset.TryParse(
                lastEventId.ToString(),
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out var cursor
            )
        )
        {
            return cursor;
        }

        return DefaultCursor;
    }

    static async IAsyncEnumerable<SseItem<TPayload>> ReadServerSentEventsAsync<
        TStreamData,
        TPayload
    >(
        ChannelReader<IntervalData<TStreamData, TPartition, DateTimeOffset>> reader,
        Func<DataResult<TStreamData, TPartition, DateTimeOffset>, TPayload> map,
        Func<TPartition, string?> eventTypeSelector,
        TimeSpan reconnectionInterval,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        await foreach (var item in reader.ReadAllAsync(cancellationToken))
        {
            if (item.TryGetData(out var data))
            {
                yield return new SseItem<TPayload>(map(data), eventTypeSelector(data.Partition))
                {
                    EventId = data.Cursor.ToString("O", CultureInfo.InvariantCulture),
                    ReconnectionInterval = reconnectionInterval,
                };
            }
            else if (item.TryGetError(out var error))
            {
                throw new InvalidOperationException(error.ErrorMessage);
            }
        }
    }
}
