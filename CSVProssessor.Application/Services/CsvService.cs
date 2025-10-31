using CSVProssessor.Application.Interfaces;
using CSVProssessor.Application.Interfaces.Common;
using CSVProssessor.Application.Utils;
using CSVProssessor.Domain.DTOs.CsvJobDTOs;
using CSVProssessor.Domain.Entities;
using CSVProssessor.Domain.Enums;
using CSVProssessor.Infrastructure.Interfaces;
using Microsoft.AspNetCore.Http;
using System.Text.Json;

namespace CSVProssessor.Application.Services
{
    public class CsvService : ICsvService
    {
        public readonly IUnitOfWork _unitOfWork;
        public readonly IBlobService _blobService;
        public readonly IRabbitMqService _rabbitMqService;

        public CsvService(IUnitOfWork unitOfWork, IBlobService blobService, IRabbitMqService rabbitMqService)
        {
            _unitOfWork = unitOfWork;
            _blobService = blobService;
            _rabbitMqService = rabbitMqService;
        }

        public async Task<ImportCsvResponseDto> ImportCsvAsync(IFormFile file)
        {
            // Kiểm tra input hợp lệ
            if (file == null)
                throw ErrorHelper.BadRequest("File không được để trống.");

            if (file.Length == 0)
                throw ErrorHelper.BadRequest("File không được rỗng.");

            if (string.IsNullOrWhiteSpace(file.FileName))
                throw ErrorHelper.BadRequest("Tên file không được để trống.");

            // Đọc file stream từ IFormFile
            using var stream = file.OpenReadStream();

            // 1. tải file lên MinIO blob storage
            await _blobService.UploadFileAsync(file.FileName, stream);

            // 2. ghi nhận job vào database
            var jobId = Guid.NewGuid();
            var csvJob = new CsvJob
            {
                Id = jobId,
                FileName = file.FileName,
                Type = CsvJobType.Import,
                Status = CsvJobStatus.Pending,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow
            };

            await _unitOfWork.CsvJobs.AddAsync(csvJob);
            await _unitOfWork.SaveChangesAsync();

            // 3. Prepare and publish message to RabbitMQ queue
            var message = new CsvImportMessage
            {
                JobId = jobId,
                FileName = file.FileName,
                UploadedAt = DateTime.UtcNow
            };

            // Publish message vào queue cho background service xử lý
            await _rabbitMqService.PublishAsync("csv-import-queue", message);

            // Return response DTO
            return new ImportCsvResponseDto
            {
                JobId = jobId,
                FileName = file.FileName,
                UploadedAt = DateTime.UtcNow,
                Status = csvJob.Status.ToString(),
                Message = "File CSV đã được tải lên thành công. Background service sẽ xử lý trong thời gian sớm nhất."
            };
        }

        /// <summary>
        /// Process CSV import: download file from MinIO, parse it, and save records to database
        /// Called by CsvImportQueueListenerService
        /// Handles: Download → Parse CSV → Save to DB → Update job status
        /// </summary>
        public async Task SaveCsvRecordsAsync(Guid jobId, string fileName)
        {
            // 1. Download file from MinIO
            using var fileStream = await _blobService.DownloadFileAsync(fileName);

            // 2. Parse CSV file
            var records = await ParseCsvAsync(jobId, fileName, fileStream);

            if (records == null || records.Count == 0)
                throw ErrorHelper.BadRequest("Không có records để lưu vào database.");

            // 3. Add all records to database
            foreach (var record in records)
            {
                await _unitOfWork.CsvRecords.AddAsync(record);
            }

            // 4. Save changes to database
            await _unitOfWork.SaveChangesAsync();

            // 5. Update job status to Completed
            var csvJob = await _unitOfWork.CsvJobs.FirstOrDefaultAsync(x => x.Id == jobId);
            if (csvJob != null)
            {
                csvJob.Status = CsvJobStatus.Completed;
                csvJob.UpdatedAt = DateTime.UtcNow;
                await _unitOfWork.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Parse CSV file into CsvRecord list
        /// </summary>
        private async Task<List<CsvRecord>> ParseCsvAsync(Guid jobId, string fileName, Stream fileStream)
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

                // Create JSON object as string and parse it
                var jsonDict = new Dictionary<string, object>();
                for (int i = 0; i < headers.Length && i < values.Length; i++)
                {
                    jsonDict[headers[i]] = values[i];
                }

                var jsonString = JsonSerializer.Serialize(jsonDict);
                var jsonDoc = JsonDocument.Parse(jsonString);

                records.Add(new CsvRecord
                {
                    JobId = jobId,
                    FileName = fileName,
                    ImportedAt = DateTime.UtcNow,
                    Data = jsonDoc
                });
            }
            return records;
        }

        //    /// <summary>
        //    /// Export data as CSV file synchronously.
        //    /// 1. Query data from PostgreSQL database
        //    /// 2. Generate CSV content
        //    /// 3. Upload CSV to MinIO blob storage
        //    /// 4. Return presigned URL for download
        //    /// </summary>
        //    public async Task<string> ExportCsvAsync(string exportFileName)
        //    {
        //        // 1. Create export job record in database
        //        var jobId = Guid.NewGuid();
        //        var csvJob = new CsvJob
        //        {
        //            Id = jobId,
        //            FileName = exportFileName,
        //            Type = CsvJobType.Export,
        //            Status = CsvJobStatus.Processing,
        //            CreatedAt = DateTime.UtcNow,
        //            UpdatedAt = DateTime.UtcNow
        //        };

        //        await _unitOfWork.CsvJobs.AddAsync(csvJob);
        //        await _unitOfWork.SaveChangesAsync();

        //        try
        //        {
        //            // 2. Query all CSV records from database
        //            var records = await _unitOfWork.CsvRecords.GetAllAsync();

        //            // 3. Generate CSV content
        //            using var ms = new MemoryStream();
        //            using (var writer = new StreamWriter(ms, leaveOpen: true))
        //            {
        //                // Write CSV header
        //                await writer.WriteLineAsync("Id,JobId,FileName,ImportedAt,Data");

        //                // Write CSV rows
        //                foreach (var record in records)
        //                {
        //                    var dataJson = record.Data?.ToString() ?? "";
        //                    var line = $"{record.Id},{record.JobId},{record.FileName},{record.ImportedAt:O},\"{dataJson}\"";
        //                    await writer.WriteLineAsync(line);
        //                }
        //                await writer.FlushAsync();
        //            }
        //            ms.Position = 0;

        //            // 4. Upload CSV to MinIO
        //            await _blobService.UploadFileAsync(exportFileName, ms);

        //            // 5. Generate presigned URL
        //            var presignedUrl = await _blobService.GetFileUrlAsync(exportFileName);

        //            // Update job status to Completed
        //            csvJob.Status = CsvJobStatus.Completed;
        //            csvJob.UpdatedAt = DateTime.UtcNow;
        //            await _unitOfWork.SaveChangesAsync();

        //            return presignedUrl;
        //        }
        //        catch (Exception ex)
        //        {
        //            // Update job status to Failed on error
        //            csvJob.Status = CsvJobStatus.Failed;
        //            csvJob.UpdatedAt = DateTime.UtcNow;
        //            await _unitOfWork.SaveChangesAsync();

        //            throw new Exception($"Export failed: {ex.Message}", ex);
        //        }


        /// <summary>
        /// Detect changes and publish notification to RabbitMQ topic "csv-changes-topic"
        /// Called by ChangeDetectionBackgroundService to check for changes and notify all instances.
        /// </summary>
        /// <param name="request">Request containing detection parameters</param>
        /// <returns>Response containing detected changes and publishing status</returns>
        public async Task<DetectChangesResponseDto> DetectAndPublishChangesAsync(DetectChangesRequestDto request)
        {
            DateTime timeThreshold;

            if (request.LastCheckTime.HasValue)
            {
                // Use provided lastCheckTime
                timeThreshold = request.LastCheckTime.Value;
            }
            else
            {
                // Calculate time threshold based on minutesBack
                timeThreshold = DateTime.UtcNow.AddMinutes(-request.MinutesBack);
            }

            // Query records created or updated since time threshold
            var records = await _unitOfWork.CsvRecords.GetAllAsync(x =>
                (x.CreatedAt >= timeThreshold || (x.UpdatedAt.HasValue && x.UpdatedAt >= timeThreshold))
                && !x.IsDeleted
            );

            bool publishedToTopic = false;

            // Publish notification if changes detected and publishToTopic is true
            if (request.PublishToTopic && records.Count > 0)
            {
                var notificationMessage = new CsvChangeNotificationMessage
                {
                    ChangeType = request.ChangeType,
                    RecordIds = records.Select(x => x.Id).ToList(),
                    TotalChanges = records.Count,
                    DetectedAt = DateTime.UtcNow,
                    CheckStartTime = timeThreshold,
                    CheckEndTime = DateTime.UtcNow,
                    InstanceName = request.InstanceName
                };

                // Publish to topic - all subscribed instances will receive this message
                await _rabbitMqService.PublishToTopicAsync("csv-changes-topic", notificationMessage);
                publishedToTopic = true;
            }

            // Map records to DTO
            var recordDtos = records.Select(x => new CsvRecordDto
            {
                Id = x.Id,
                JobId = x.JobId,
                FileName = x.FileName,
                ImportedAt = x.ImportedAt,
                CreatedAt = x.CreatedAt,
                UpdatedAt = x.UpdatedAt
            }).ToList();

            // Build response
            var response = new DetectChangesResponseDto
            {
                Changes = recordDtos,
                TotalChanges = records.Count,
                PublishedToTopic = publishedToTopic,
                DetectedAt = DateTime.UtcNow,
                CheckStartTime = timeThreshold,
                CheckEndTime = DateTime.UtcNow,
                Message = records.Count > 0
                    ? $"Detected {records.Count} changes. Published: {publishedToTopic}"
                    : "No changes detected"
            };

            return response;
        }
    }
}