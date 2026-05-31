using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace PollR.Samples.WebShared;

/// <summary>
/// Extension class that provides the common setup.
/// </summary>
public static class PollRSetupExtensions
{
    extension(IServiceCollection services)
    {
        /// <summary>
        /// You can break this up in your implementation however you want; we keep
        /// this here to simplify the setup for the demos.
        /// </summary>
        public IServiceCollection AddPollR(string databaseName)
        {
            // The ASP.NET Core adapter reads Last-Event-ID and request cancellation from the current context.
            services.AddHttpContextAccessor();

            // The sample uses EF Core InMemory as the source of truth for the stream.
            services.AddDbContextFactory<MessageDb>(options =>
                options.UseInMemoryDatabase("PollR-web-minimal")
            );

            // The poller is a singleton so all SSE subscribers on this node share one polling loop.
            services.AddSingleton<MessagePoller>();

            services.AddSingleton<PollRCaster<MessageEvent, string>>(provider =>
                provider
                    .GetRequiredService<MessagePoller>()
                    .RegisterSerializedProjection(data => JsonSerializer.Serialize(data.Data))
            );

            // Start the singleton poller with the app and dispose it during normal host shutdown.
            services.AddHostedService<MessagePollerHostedService>();

            return services;
        }
    }
}
