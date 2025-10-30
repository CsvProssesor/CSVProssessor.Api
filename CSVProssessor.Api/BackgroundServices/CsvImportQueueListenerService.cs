using CSVProssessor.Application.Interfaces.Common;
using CSVProssessor.Domain;
using CSVProssessor.Domain.Entities;
using CSVProssessor.Domain.Enums;
using CSVProssessor.Infrastructure.Interfaces;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CSVProssessor.Api.BackgroundServices
{
    /// <summary>
    /// Background service that listens to csv-import-queue and processes CSV import messages.
    /// Competing consumer pattern - multiple instances will share the workload.
    /// </summary>
    public class CsvImportQueueListenerService : BackgroundService
    {
        private readonly IConnection _rabbitMqConnection;
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<CsvImportQueueListenerService> _logger;
        private IChannel? _channel;
        private readonly string _queueName;
        private readonly string _instanceName;

        public CsvImportQueueListenerService(
            IConnection rabbitMqConnection,
            IServiceProvider serviceProvider,
            ILogger<CsvImportQueueListenerService> logger,
            IConfiguration configuration)
        {
            _rabbitMqConnection = rabbitMqConnection;
            _serviceProvider = serviceProvider;
            _logger = logger;
            _queueName = Environment.GetEnvironmentVariable("RABBITMQ_QUEUE_NAME") ?? configuration["RabbitMQ:QueueName"] ?? "csv-import-queue";
            _instanceName = Environment.GetEnvironmentVariable("INSTANCE_NAME") ?? "api-unknown";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"[{_instanceName}] CsvImportQueueListenerService starting...");

            try
            {
                // Create channel
                _channel = await _rabbitMqConnection.CreateChannelAsync(cancellationToken: stoppingToken);

                // Declare queue with durable option
                await _channel.QueueDeclareAsync(
                    queue: _queueName,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: stoppingToken
                );

                // Set prefetch count for load balancing between instances
                await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false, cancellationToken: stoppingToken);

                _logger.LogInformation($"[{_instanceName}] Listening to queue '{_queueName}' with prefetch count 10");

                // Create consumer
                var consumer = new AsyncEventingBasicConsumer(_channel);
                consumer.ReceivedAsync += async (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var jsonMessage = Encoding.UTF8.GetString(body);

                    _logger.LogInformation($"[{_instanceName}] Received message from queue '{_queueName}': {jsonMessage}");

                    try
                    {
                        // Process the message
                        await ProcessCsvImportMessageAsync(jsonMessage, stoppingToken);

                        // ACK only after successful processing
                        await _channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false, cancellationToken: stoppingToken);
                        _logger.LogInformation($"[{_instanceName}] Message processed and ACKed successfully.");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"[{_instanceName}] Error processing message: {ex.Message}. Message will NOT be ACKed for reprocessing.");
                        // Do NOT ACK - message will be redelivered by RabbitMQ
                        // Optionally, you can implement NACK with requeue:
                        // await _channel.BasicNackAsync(ea.DeliveryTag, false, true, stoppingToken);
                    }
                };

                // Start consuming
                await _channel.BasicConsumeAsync(
                    queue: _queueName,
                    autoAck: false, // Manual ACK
                    consumer: consumer,
                    cancellationToken: stoppingToken
                );

                _logger.LogInformation($"[{_instanceName}] Successfully started consuming from queue '{_queueName}'");

                // Keep the service running
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation($"[{_instanceName}] CsvImportQueueListenerService is stopping due to cancellation.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[{_instanceName}] Fatal error in CsvImportQueueListenerService: {ex.Message}");
                throw;
            }
        }

        private async Task ProcessCsvImportMessageAsync(string jsonMessage, CancellationToken cancellationToken)
        {
            // Deserialize message
            var message = JsonSerializer.Deserialize<CsvImportMessage>(jsonMessage);
            if (message == null || string.IsNullOrWhiteSpace(message.Filename))
            {
                _logger.LogWarning($"[{_instanceName}] Invalid message format. Skipping.");
                return;
            }

            _logger.LogInformation($"[{_instanceName}] Processing CSV import for JobId: {message.JobId}, File: {message.Filename}");

            // Create a new scope for scoped services
            using var scope = _serviceProvider.CreateScope();
            var blobService = scope.ServiceProvider.GetRequiredService<IBlobService>();
            var dbContext = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            try
            {
                // 1. Update job status to Processing
                var csvJob = await dbContext.CsvJobs.FindAsync(new object[] { message.JobId }, cancellationToken);
                if (csvJob != null)
                {
                    csvJob.Status = CsvJobStatus.Processing;
                    csvJob.UpdatedAt = DateTime.UtcNow;
                    await dbContext.SaveChangesAsync(cancellationToken);
                }

                // 2. Download file from MinIO
                _logger.LogInformation($"[{_instanceName}] Downloading file '{message.Filename}' from MinIO...");
                await using var fileStream = await blobService.DownloadFileAsync(message.Filename);

                // 3. Parse CSV file
                using var reader = new StreamReader(fileStream);
                var csvRecords = new List<CsvRecord>();

                // Read header line
                var headerLine = await reader.ReadLineAsync(cancellationToken);
                if (string.IsNullOrWhiteSpace(headerLine))
                {
                    throw new InvalidOperationException("CSV file is empty or has no header.");
                }

                var headers = headerLine.Split(',').Select(h => h.Trim()).ToArray();
                _logger.LogInformation($"[{_instanceName}] CSV Headers: {string.Join(", ", headers)}");

                int rowCount = 0;
                string? line;
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var values = line.Split(',').Select(v => v.Trim()).ToArray();

                    // Create JsonObject for dynamic data storage
                    var jsonData = new JsonObject();
                    for (int i = 0; i < headers.Length && i < values.Length; i++)
                    {
                        jsonData[headers[i]] = values[i];
                    }

                    // Create CsvRecord entity
                    var csvRecord = new CsvRecord
                    {
                        Id = Guid.NewGuid(),
                        JobId = message.JobId,
                        FileName = message.Filename,
                        ImportedAt = DateTime.UtcNow,
                        Data = jsonData,
                        CreatedAt = DateTime.UtcNow,
                        UpdatedAt = DateTime.UtcNow
                    };

                    csvRecords.Add(csvRecord);
                    rowCount++;
                }

                // 4. Save all records to database using bulk insert
                _logger.LogInformation($"[{_instanceName}] Saving {csvRecords.Count} records to database...");
                await dbContext.CsvRecords.AddRangeAsync(csvRecords, cancellationToken);

                // 5. Update job status to Completed
                if (csvJob != null)
                {
                    csvJob.Status = CsvJobStatus.Completed;
                    csvJob.UpdatedAt = DateTime.UtcNow;
                }

                await dbContext.SaveChangesAsync(cancellationToken);

                _logger.LogInformation($"[{_instanceName}] Successfully processed CSV import. JobId: {message.JobId}, Records: {rowCount}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[{_instanceName}] Error processing CSV import for JobId: {message.JobId}");

                // Update job status to Failed
                try
                {
                    var csvJob = await dbContext.CsvJobs.FindAsync(new object[] { message.JobId }, cancellationToken);
                    if (csvJob != null)
                    {
                        csvJob.Status = CsvJobStatus.Failed;
                        csvJob.UpdatedAt = DateTime.UtcNow;
                        await dbContext.SaveChangesAsync(cancellationToken);
                    }
                }
                catch (Exception dbEx)
                {
                    _logger.LogError(dbEx, $"[{_instanceName}] Failed to update job status to Failed for JobId: {message.JobId}");
                }

                throw; // Re-throw to prevent ACK
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"[{_instanceName}] CsvImportQueueListenerService is stopping...");
            
            if (_channel != null)
            {
                await _channel.CloseAsync(cancellationToken);
                _channel.Dispose();
            }

            await base.StopAsync(cancellationToken);
        }

        /// <summary>
        /// Message structure for CSV import queue
        /// </summary>
        private class CsvImportMessage
        {
            public Guid JobId { get; set; }
            public string Filename { get; set; } = string.Empty;
            public DateTime UploadedAt { get; set; }
        }
    }
}
