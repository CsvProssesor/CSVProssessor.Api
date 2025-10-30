using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace CSVProssessor.Api.BackgroundServices
{
    /// <summary>
    /// Background service that listens to csv-changes-topic to receive change notifications.
    /// Fan-out pattern - all instances receive the same messages.
    /// </summary>
    public class CsvChangesTopicListenerService : BackgroundService
    {
        private readonly IConnection _rabbitMqConnection;
        private readonly ILogger<CsvChangesTopicListenerService> _logger;
        private IChannel? _channel;
        private readonly string _topicName;
        private readonly string _instanceName;
        private string? _queueName;

        public CsvChangesTopicListenerService(
            IConnection rabbitMqConnection,
            ILogger<CsvChangesTopicListenerService> logger,
            IConfiguration configuration)
        {
            _rabbitMqConnection = rabbitMqConnection;
            _logger = logger;
            _topicName = Environment.GetEnvironmentVariable("RABBITMQ_TOPIC_NAME") ?? configuration["RabbitMQ:TopicName"] ?? "csv-changes-topic";
            _instanceName = Environment.GetEnvironmentVariable("INSTANCE_NAME") ?? "api-unknown";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"[{_instanceName}] CsvChangesTopicListenerService starting...");

            try
            {
                // Create channel
                _channel = await _rabbitMqConnection.CreateChannelAsync(cancellationToken: stoppingToken);

                // Declare the fanout exchange (topic)
                await _channel.ExchangeDeclareAsync(
                    exchange: _topicName,
                    type: ExchangeType.Fanout,
                    durable: true,
                    autoDelete: false,
                    arguments: null,
                    cancellationToken: stoppingToken
                );

                // Create an instance-specific queue (exclusive to this instance)
                // Empty queue name lets RabbitMQ generate a unique name
                var queueDeclareResult = await _channel.QueueDeclareAsync(
                    queue: string.Empty, // Auto-generated unique name
                    durable: false,      // Not durable - temporary for this instance
                    exclusive: true,     // Exclusive to this connection
                    autoDelete: true,    // Auto-delete when consumer disconnects
                    arguments: null,
                    cancellationToken: stoppingToken
                );

                _queueName = queueDeclareResult.QueueName;
                _logger.LogInformation($"[{_instanceName}] Created instance-specific queue: {_queueName}");

                // Bind queue to the topic exchange
                await _channel.QueueBindAsync(
                    queue: _queueName,
                    exchange: _topicName,
                    routingKey: string.Empty, // Fanout exchange ignores routing keys
                    arguments: null,
                    cancellationToken: stoppingToken
                );

                _logger.LogInformation($"[{_instanceName}] Bound queue '{_queueName}' to topic '{_topicName}'");

                // Create consumer
                var consumer = new AsyncEventingBasicConsumer(_channel);
                consumer.ReceivedAsync += async (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var jsonMessage = Encoding.UTF8.GetString(body);

                    _logger.LogInformation($"[{_instanceName}] ===== CHANGE NOTIFICATION RECEIVED =====");
                    _logger.LogInformation($"[{_instanceName}] Topic: {_topicName}");
                    _logger.LogInformation($"[{_instanceName}] Message: {jsonMessage}");
                    _logger.LogInformation($"[{_instanceName}] ==========================================");

                    // Auto-ACK is enabled for topic listeners since messages are informational
                    await Task.CompletedTask;
                };

                // Start consuming (auto-ACK enabled)
                await _channel.BasicConsumeAsync(
                    queue: _queueName,
                    autoAck: true, // Auto-ACK for topic messages
                    consumer: consumer,
                    cancellationToken: stoppingToken
                );

                _logger.LogInformation($"[{_instanceName}] Successfully started listening to topic '{_topicName}'");

                // Keep the service running
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation($"[{_instanceName}] CsvChangesTopicListenerService is stopping due to cancellation.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[{_instanceName}] Fatal error in CsvChangesTopicListenerService: {ex.Message}");
                throw;
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"[{_instanceName}] CsvChangesTopicListenerService is stopping...");

            if (_channel != null)
            {
                await _channel.CloseAsync(cancellationToken);
                _channel.Dispose();
            }

            await base.StopAsync(cancellationToken);
        }
    }
}
