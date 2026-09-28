using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace OutboxPattern;

/// <summary>Background loop that drains the outbox. Register with <c>services.AddHostedService&lt;OutboxWorker&gt;()</c>.</summary>
public class OutboxWorker(IServiceScopeFactory scopes, TimeSpan? pollInterval = null) : BackgroundService
{
    private readonly TimeSpan _poll = pollInterval ?? TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = scopes.CreateScope();
                var dispatcher = scope.ServiceProvider.GetRequiredService<OutboxDispatcher>();

                // Keep draining while there is work; sleep only when idle.
                while (await dispatcher.ProcessBatchAsync(stoppingToken) > 0) { }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch
            {
                // A transient database error must not kill the worker; it will retry on the next tick.
            }

            await Task.Delay(_poll, stoppingToken);
        }
    }
}
