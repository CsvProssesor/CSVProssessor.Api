using CSVProssessor.Application.Interfaces;
using CSVProssessor.Domain.DTOs.CsvJobDTOs;
using CSVProssessor.Infrastructure.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CSVProssessor.Application.Worker
{
    /// <summary>
    /// Background service that runs periodically (every 5 minutes) to detect changes in CSV records
    /// and publish notifications to RabbitMQ topic "csv-changes-topic".
    /// All instances (api-1, api-2) subscribe to this topic and receive notifications (fan-out pattern).
    /// </summary>
    public class ChangeDetectionBackgroundService : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private const int CheckIntervalMinutes = 1;

        public ChangeDetectionBackgroundService(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var scope = _serviceProvider.CreateScope();
            var loggerService = scope.ServiceProvider.GetRequiredService<ILoggerService>();

            loggerService.Info("ChangeDetectionBackgroundService started");

            // Get instance name from environment variable
            var instanceName = Environment.GetEnvironmentVariable("INSTANCE_NAME") ?? "api-unknown";

            try
            {
                // Initial delay to allow all services to start
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

                // Keep running and check periodically
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        // Create a new scope for each check to resolve scoped services
                        using var checkScope = _serviceProvider.CreateScope();
                        var csvService = checkScope.ServiceProvider.GetRequiredService<ICsvService>();
                        var scopedLogger = checkScope.ServiceProvider.GetRequiredService<ILoggerService>();

                        scopedLogger.Info($"[{instanceName}] Starting change detection check...");

                        // Create request DTO
                        var request = new DetectChangesRequestDto
                        {
                            LastCheckTime = null, // Will use minutesBack
                            MinutesBack = CheckIntervalMinutes,
                            ChangeType = "Created",
                            InstanceName = instanceName,
                            PublishToTopic = true
                        };

                        // Call the detect and publish method
                        var response = await csvService.DetectAndPublishChangesAsync(request);

                        // Log results
                        if (response.TotalChanges > 0)
                        {
                            scopedLogger.Success(
                                $"[{instanceName}] {response.Message} - Changes: {response.TotalChanges}, " +
                                $"Published: {response.PublishedToTopic}");
                        }
                        else
                        {
                            scopedLogger.Info($"[{instanceName}] {response.Message}");
                        }
                    }
                    catch (Exception ex)
                    {
                        using var errorScope = _serviceProvider.CreateScope();
                        var errorLogger = errorScope.ServiceProvider.GetRequiredService<ILoggerService>();
                        errorLogger.Error($"[{instanceName}] Error during change detection: {ex.Message}\n{ex.StackTrace}");
                        // Continue running even if one check fails
                    }

                    // Wait for the next check interval
                    using var waitScope = _serviceProvider.CreateScope();
                    var waitLogger = waitScope.ServiceProvider.GetRequiredService<ILoggerService>();
                    waitLogger.Info($"[{instanceName}] Waiting {CheckIntervalMinutes} minutes until next check...");

                    await Task.Delay(TimeSpan.FromMinutes(CheckIntervalMinutes), stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                using var cancelScope = _serviceProvider.CreateScope();
                var cancelLogger = cancelScope.ServiceProvider.GetRequiredService<ILoggerService>();
                cancelLogger.Info("ChangeDetectionBackgroundService is shutting down gracefully");
            }
            catch (Exception ex)
            {
                using var fatalScope = _serviceProvider.CreateScope();
                var fatalLogger = fatalScope.ServiceProvider.GetRequiredService<ILoggerService>();
                fatalLogger.Error($"Fatal error in ChangeDetectionBackgroundService: {ex.Message}\n{ex.StackTrace}");
                throw;
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            using var scope = _serviceProvider.CreateScope();
            var loggerService = scope.ServiceProvider.GetRequiredService<ILoggerService>();
            loggerService.Info("ChangeDetectionBackgroundService stopped");
            await base.StopAsync(cancellationToken);
        }
    }
}
