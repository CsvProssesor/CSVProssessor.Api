using CSVProssessor.Application.Interfaces.Common;
using CSVProssessor.Domain.DTOs.CsvJobDTOs;
using CSVProssessor.Domain.Entities;
using CSVProssessor.Infrastructure.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CSVProssessor.Application.Worker
{
    public class CsvImportQueueListenerService : BackgroundService
    {
        private readonly ILoggerService _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IConnection _rabbitConnection;
        private readonly IBlobService _blobService;
        private readonly IUnitOfWork _unitOfWork;
        public CsvImportQueueListenerService(ILoggerService logger, IServiceProvider serviceProvider, IConnection rabbitConnection, IBlobService blobService, IUnitOfWork unitOfWork)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _rabbitConnection = rabbitConnection;
            _blobService = blobService;
            _unitOfWork = unitOfWork;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
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
                using var scope = _serviceProvider.CreateScope();

                try
                {
                    // Giải mã message từ byte array
                    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
                    var message = JsonSerializer.Deserialize<CsvImportMessage>(json);

                    if (message == null)
                    {
                        _logger.Warn("Received null or invalid CsvImportMessage.");
                        await channel.BasicAckAsync(ea.DeliveryTag, false);
                        return;
                    }

                    _logger.Info($"Processing CSV import for file: {message.FileName}, JobId: {message.JobId}");

                    // Download file from MinIO
                    using var fileStream = await _blobService.DownloadFileAsync(message.FileName);

                    // Parse CSV and save to DB
                    var records = await ParseCsvAsync(message, fileStream);

                    foreach (var record in records)
                    {
                        await _unitOfWork.CsvRecords.AddAsync(record);
                    }
                    await _unitOfWork.SaveChangesAsync();

                    _logger.Info($"Imported {records.Count} records from {message.FileName}");

                    // ACK - xác nhận đã xử lý thành công
                    await channel.BasicAckAsync(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    _logger.Error($"Error processing CSV import message + {ex}");
                    // NACK và requeue - đưa message trở lại queue để retry
                    await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
                }
            }

            // Bắt đầu consume message từ queue
            await channel.BasicConsumeAsync(
                queue: "csv-import-queue",
                autoAck: false,
                consumer: consumer);

            _logger.Info("Started listening to csv-import-queue");

            // Giữ service chạy cho đến khi bị cancel
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }


        private async Task<List<CsvRecord>> ParseCsvAsync(CsvImportMessage message, Stream fileStream)
        {
            var records = new List<CsvRecord>();
            using var reader = new StreamReader(fileStream);

            // Đọc header
            var headerLine = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(headerLine))
                return records;

            var headers = headerLine.Split(',').Select(h => h.Trim()).ToArray();

            string? line;
            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var values = line.Split(',').Select(v => v.Trim()).ToArray();

                var json = new JsonObject();
                for (int i = 0; i < headers.Length && i < values.Length; i++)
                {
                    json[headers[i]] = values[i];
                }

                records.Add(new CsvRecord
                {
                    JobId = message.JobId,
                    FileName = message.FileName,
                    ImportedAt = DateTime.UtcNow,
                    Data = json
                });
            }
            return records;
        }

    }
}
