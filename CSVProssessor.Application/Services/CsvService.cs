using CSVProssessor.Application.Interfaces;
using CSVProssessor.Application.Interfaces.Common;
using CSVProssessor.Domain.Entities;
using CSVProssessor.Domain.Enums;
using CSVProssessor.Infrastructure.Interfaces;

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

        /// <summary>
        /// Import CSV asynchronously.
        /// 1. Upload file to MinIO
        /// 2. Create CsvJob record in database with Pending status
        /// 3. Publish message to RabbitMQ queue for background processing
        /// </summary>
        public async Task<Guid> ImportCsvAsync(Stream fileStream, string fileName)
        {
            // 1. Upload file to MinIO blob storage
            await _blobService.UploadFileAsync(fileName, fileStream);

            // 2. Create job record in database
            var jobId = Guid.NewGuid();
            var csvJob = new CsvJob
            {
                Id = jobId,
                FileName = fileName,
                Type = CsvJobType.Import,
                Status = CsvJobStatus.Pending,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow
            };

            // Add job to repository and save
            await _unitOfWork.CsvJobRepository.AddAsync(csvJob);
            await _unitOfWork.SaveChangesAsync();

            // 3. Prepare and publish message to RabbitMQ queue
            var message = new
            {
                jobId = jobId,
                filename = fileName,
                uploadedAt = DateTime.UtcNow
            };

            // Publish to csv-import-queue for background service to consume
            await _rabbitMqService.PublishAsync("csv-import-queue", message);

            // Return job ID for tracking
            return jobId;
        }

        /// <summary>
        /// Export data as CSV file synchronously.
        /// 1. Query data from PostgreSQL database
        /// 2. Generate CSV content
        /// 3. Upload CSV to MinIO blob storage
        /// 4. Return presigned URL for download
        /// </summary>
        public async Task<string> ExportCsvAsync(string exportFileName)
        {
            // 1. Create export job record in database
            var jobId = Guid.NewGuid();
            var csvJob = new CsvJob
            {
                Id = jobId,
                FileName = exportFileName,
                Type = CsvJobType.Export,
                Status = CsvJobStatus.Processing,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow
            };

            await _unitOfWork.CsvJobRepository.AddAsync(csvJob);
            await _unitOfWork.SaveChangesAsync();

            try
            {
                // 2. Query all CSV records from database
                var records = await _unitOfWork.CsvRecordRepository.GetAllAsync();

                // 3. Generate CSV content
                using var ms = new MemoryStream();
                using (var writer = new StreamWriter(ms, leaveOpen: true))
                {
                    // Write CSV header
                    await writer.WriteLineAsync("Id,JobId,FileName,ImportedAt,Data");

                    // Write CSV rows
                    foreach (var record in records)
                    {
                        var dataJson = record.Data?.ToString() ?? "";
                        var line = $"{record.Id},{record.JobId},{record.FileName},{record.ImportedAt:O},\"{dataJson}\"";
                        await writer.WriteLineAsync(line);
                    }
                    await writer.FlushAsync();
                }
                ms.Position = 0;

                // 4. Upload CSV to MinIO
                await _blobService.UploadFileAsync(exportFileName, ms);

                // 5. Generate presigned URL
                var presignedUrl = await _blobService.GetFileUrlAsync(exportFileName);

                // Update job status to Completed
                csvJob.Status = CsvJobStatus.Completed;
                csvJob.UpdatedAt = DateTime.UtcNow;
                await _unitOfWork.SaveChangesAsync();

                return presignedUrl;
            }
            catch (Exception ex)
            {
                // Update job status to Failed on error
                csvJob.Status = CsvJobStatus.Failed;
                csvJob.UpdatedAt = DateTime.UtcNow;
                await _unitOfWork.SaveChangesAsync();

                throw new Exception($"Export failed: {ex.Message}", ex);
            }
        }
    }
}