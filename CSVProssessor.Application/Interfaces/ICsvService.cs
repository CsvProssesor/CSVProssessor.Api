namespace CSVProssessor.Application.Interfaces
{
    using CSVProssessor.Domain.DTOs.CsvJobDTOs;
    using CSVProssessor.Domain.Entities;
    using Microsoft.AspNetCore.Http;

    public interface ICsvService
    {
        /// <summary>
        /// Import CSV asynchronously.
        /// Upload file to MinIO
        /// Create CsvJob record in database with Pending status
        /// Publish message to RabbitMQ queue for background processing
        /// </summary>
        Task<ImportCsvResponseDto> ImportCsvAsync(IFormFile file);

        /// <summary>
        /// Process CSV import: download file from MinIO, parse it, and save records to database
        /// Called by CsvImportQueueListenerService
        /// Handles: Download → Parse CSV → Save to DB → Update job status
        /// </summary>
        Task SaveCsvRecordsAsync(Guid jobId, string fileName);

        /// <summary>
        /// Detect changes and publish notification to RabbitMQ topic "csv-changes-topic"
        /// Called by ChangeDetectionBackgroundService to check for changes and notify all instances.
        /// </summary>
        Task<DetectChangesResponseDto> DetectAndPublishChangesAsync(DetectChangesRequestDto request);

        //download
        //Task<string> ExportCsvAsync(string exportFileName);
    }
}