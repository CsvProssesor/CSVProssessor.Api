using CSVProssessor.Application.Interfaces;
using CSVProssessor.Domain.DTOs.CsvJobDTOs;
using CSVProssessor.Infrastructure.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace CSVProssessor.Application.Worker
{
    public class CsvImportQueueListenerService : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IConnection _rabbitConnection;

        public CsvImportQueueListenerService(IServiceProvider serviceProvider, IConnection rabbitConnection)
        {
            _serviceProvider = serviceProvider;
            _rabbitConnection = rabbitConnection;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var scope = _serviceProvider.CreateScope();
            var logger = scope.ServiceProvider.GetRequiredService<ILoggerService>();

            // Tạo channel từ connection (await vì là async method)
            var channel = await _rabbitConnection.CreateChannelAsync();

            // Khai báo queue
            await channel.QueueDeclareAsync(
                queue: "csv-import-queue",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            // Thiết lập QoS - mỗi consumer nhận tối đa 1 message
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

            // Tạo consumer để xử lý message
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += OnMessageReceived;

            // Hàm async riêng để xử lý message
            async Task OnMessageReceived(object model, BasicDeliverEventArgs ea)
            {
                // Create a new scope for each message to resolve scoped services
                using var messageScope = _serviceProvider.CreateScope();
                var csvService = messageScope.ServiceProvider.GetRequiredService<ICsvService>();
                var scopedLogger = messageScope.ServiceProvider.GetRequiredService<ILoggerService>();

                try
                {
                    // Giải mã message từ byte array
                    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                    scopedLogger.Info($"Received raw message: {json}");

                    // Deserialize với PropertyNameCaseInsensitive để handle lowercase properties
                    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                    var message = JsonSerializer.Deserialize<CsvImportMessage>(json, options);

                    if (message == null)
                    {
                        scopedLogger.Warn("Received null or invalid CsvImportMessage.");
                        await channel.BasicAckAsync(ea.DeliveryTag, false);
                        return;
                    }

                    scopedLogger.Info($"Deserialized message - JobId: {message.JobId}, FileName: {message.FileName}");

                    if (string.IsNullOrWhiteSpace(message.FileName))
                    {
                        scopedLogger.Warn("FileName is empty, skipping processing.");
                        await channel.BasicAckAsync(ea.DeliveryTag, false);
                        return;
                    }

                    scopedLogger.Info($"Processing CSV import for file: {message.FileName}, JobId: {message.JobId}");

                    // Call CsvService to handle download, parse, and save to database
                    await csvService.SaveCsvRecordsAsync(message.JobId, message.FileName);

                    scopedLogger.Success($"Successfully imported CSV file: {message.FileName}");

                    // ACK - xác nhận đã xử lý thành công
                    await channel.BasicAckAsync(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    scopedLogger.Error($"Error processing CSV import message: {ex}");
                    // NACK và requeue - đưa message trở lại queue để retry
                    await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
                }
            }

            // Bắt đầu consume message từ queue
            await channel.BasicConsumeAsync(
                queue: "csv-import-queue",
                autoAck: false,
                consumer: consumer);

            logger.Info("Started listening to csv-import-queue");

            // Giữ service chạy cho đến khi bị cancel
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
    }
}
