using CSVProssessor.Infrastructure.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using System.Text.Json;

namespace CSVProssessor.Infrastructure.Commons
{
    public class RabbitMqService : IRabbitMqService
    {
        private readonly IConnection _connection;
        private readonly IChannel _channel;
        private readonly ILogger<RabbitMqService> _logger;

        public RabbitMqService(IConfiguration configuration, ILogger<RabbitMqService> logger)
        {
            _logger = logger;

            try
            {
                // Read RabbitMQ configuration from environment variables or appsettings
                var rabbitMqHost = configuration["RABBITMQ_HOST"] ?? configuration["RabbitMQ:Host"] ?? "localhost";
                var rabbitMqPort = int.Parse(configuration["RABBITMQ_PORT"] ?? configuration["RabbitMQ:Port"] ?? "5672");
                var rabbitMqUser = configuration["RABBITMQ_USER"] ?? configuration["RabbitMQ:User"] ?? "guest";
                var rabbitMqPassword = configuration["RABBITMQ_PASSWORD"] ?? configuration["RabbitMQ:Password"] ?? "guest";

                var factory = new ConnectionFactory
                {
                    HostName = rabbitMqHost,
                    Port = rabbitMqPort,
                    UserName = rabbitMqUser,
                    Password = rabbitMqPassword,
                    AutomaticRecoveryEnabled = true,
                    NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
                };

                _connection = factory.CreateConnectionAsync().Result;
                _channel = _connection.CreateChannelAsync().Result;

                _logger.LogInformation($"Successfully connected to RabbitMQ at {rabbitMqHost}:{rabbitMqPort}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize RabbitMQ connection");
                throw;
            }
        }

        /// <summary>
        /// Publishes a message to a specified RabbitMQ queue or exchange (topic).
        /// </summary>
        public async Task PublishAsync<T>(string destination, T message)
        {
            try
            {
                // Serialize the message to JSON
                var jsonMessage = JsonSerializer.Serialize(message);
                var body = System.Text.Encoding.UTF8.GetBytes(jsonMessage);

                // Declare the destination queue (idempotent - won't fail if already exists)
                // This is used for direct queue publishing
                await _channel.QueueDeclareAsync(
                    queue: destination,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null
                );

                // Publish the message to the queue
                var properties = new BasicProperties
                {
                    Persistent = true,
                    ContentType = "application/json"
                };

                await _channel.BasicPublishAsync(
                    exchange: "",  // Use default exchange for direct queue publishing
                    routingKey: destination,
                    mandatory: false,
                    basicProperties: properties,
                    body: new ReadOnlyMemory<byte>(body)
                );

                _logger.LogInformation($"Message published to {destination}: {jsonMessage}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error publishing message to {destination}");
                throw;
            }
        }

        public void Dispose()
        {
            _channel?.Dispose();
            _connection?.Dispose();
        }
    }
}