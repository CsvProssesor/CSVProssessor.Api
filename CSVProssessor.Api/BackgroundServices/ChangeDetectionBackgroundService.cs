using CSVProssessor.Domain;
using CSVProssessor.Domain.Entities;
using CSVProssessor.Infrastructure.Interfaces;
using Microsoft.EntityFrameworkCore;

namespace CSVProssessor.Api.BackgroundServices
{
    /// <summary>
    /// Background service that runs every 5 minutes to detect changes in CsvRecords table
    /// and publishes notifications to a RabbitMQ topic for all instances to receive.
    /// </summary>
    public class ChangeDetectionBackgroundService : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<ChangeDetectionBackgroundService> _logger;
        private readonly string _topicName;
        private readonly string _instanceName;
        private readonly TimeSpan _interval;
        private DateTime _lastCheckTime;

        public ChangeDetectionBackgroundService(
            IServiceProvider serviceProvider,
            ILogger<ChangeDetectionBackgroundService> logger,
            IConfiguration configuration)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _topicName = Environment.GetEnvironmentVariable("RABBITMQ_TOPIC_NAME") ?? configuration["RabbitMQ:TopicName"] ?? "csv-changes-topic";
            _instanceName = Environment.GetEnvironmentVariable("INSTANCE_NAME") ?? "api-unknown";
            _interval = TimeSpan.FromMinutes(5);
            _lastCheckTime = DateTime.UtcNow;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"[{_instanceName}] ChangeDetectionBackgroundService starting with {_interval.TotalMinutes} minute interval...");

            // Wait a bit before first run to allow system to stabilize
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await DetectAndPublishChangesAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"[{_instanceName}] Error during change detection: {ex.Message}");
                }

                // Wait for the next interval
                try
                {
                    await Task.Delay(_interval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation($"[{_instanceName}] ChangeDetectionBackgroundService is stopping...");
                    break;
                }
            }
        }

        private async Task DetectAndPublishChangesAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"[{_instanceName}] Checking for changes since {_lastCheckTime:yyyy-MM-dd HH:mm:ss}...");

            using var scope = _serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            var rabbitMqService = scope.ServiceProvider.GetRequiredService<IRabbitMqService>();

            try
            {
                var currentCheckTime = DateTime.UtcNow;

                // Query for new or updated CSV records since last check
                var newRecords = await dbContext.CsvRecords
                    .Where(r => r.CreatedAt >= _lastCheckTime && r.CreatedAt < currentCheckTime)
                    .Select(r => new
                    {
                        r.Id,
                        r.JobId,
                        r.FileName,
                        r.ImportedAt,
                        r.CreatedAt
                    })
                    .ToListAsync(cancellationToken);

                var updatedRecords = await dbContext.CsvRecords
                    .Where(r => r.UpdatedAt >= _lastCheckTime && r.UpdatedAt < currentCheckTime && r.CreatedAt < _lastCheckTime)
                    .Select(r => new
                    {
                        r.Id,
                        r.JobId,
                        r.FileName,
                        r.UpdatedAt
                    })
                    .ToListAsync(cancellationToken);

                // Check for completed jobs
                var completedJobs = await dbContext.CsvJobs
                    .Where(j => j.UpdatedAt >= _lastCheckTime && j.UpdatedAt < currentCheckTime)
                    .Select(j => new
                    {
                        j.Id,
                        j.FileName,
                        j.Type,
                        j.Status,
                        j.UpdatedAt
                    })
                    .ToListAsync(cancellationToken);

                // Prepare change notification message
                var changeNotification = new
                {
                    instanceName = _instanceName,
                    checkTime = currentCheckTime,
                    lastCheckTime = _lastCheckTime,
                    newRecordsCount = newRecords.Count,
                    updatedRecordsCount = updatedRecords.Count,
                    completedJobsCount = completedJobs.Count,
                    newRecords = newRecords.Take(10), // Limit to 10 for brevity
                    completedJobs = completedJobs
                };

                // Only publish if there are changes
                if (newRecords.Any() || updatedRecords.Any() || completedJobs.Any())
                {
                    _logger.LogInformation(
                        $"[{_instanceName}] Detected changes - New: {newRecords.Count}, Updated: {updatedRecords.Count}, Jobs: {completedJobs.Count}");

                    // Publish to topic - all instances will receive this
                    await rabbitMqService.PublishToTopicAsync(_topicName, changeNotification);

                    _logger.LogInformation($"[{_instanceName}] Change notification published to topic '{_topicName}'");
                }
                else
                {
                    _logger.LogInformation($"[{_instanceName}] No changes detected since last check.");
                }

                // Update last check time
                _lastCheckTime = currentCheckTime;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[{_instanceName}] Error detecting or publishing changes: {ex.Message}");
                throw;
            }
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"[{_instanceName}] ChangeDetectionBackgroundService is stopping...");
            return base.StopAsync(cancellationToken);
        }
    }
}
