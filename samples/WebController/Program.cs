using Microsoft.EntityFrameworkCore;
using PollR;
using PollR.Samples.WebShared;

var builder = WebApplication.CreateBuilder(args);

// The ASP.NET Core adapter reads Last-Event-ID and request cancellation from the current context.
builder.Services.AddHttpContextAccessor();
builder.Services.AddControllers();

// The sample uses EF Core InMemory as the source of truth for the stream.
builder.Services.AddDbContextFactory<MessageDb>(options =>
    options.UseInMemoryDatabase("PollR-web-controller")
);

// The poller is a singleton so all SSE subscribers on this node share one polling loop.
builder.Services.AddSingleton<MessagePoller>();
builder.Services.AddSingleton<PollRCaster<MessageEvent, string>>(provider =>
    provider.GetRequiredService<MessagePoller>().Poller
);

// Start the singleton poller with the app and dispose it during normal host shutdown.
builder.Services.AddHostedService<MessagePollerHostedService>();

var app = builder.Build();

app.MapControllers();

app.Run();
