namespace CSVProssessor.Application.Interfaces
{
    public interface ICsvService
    {
        /// <summary>
        /// Import CSV file asynchronously.
        /// Uploads file to blob storage and publishes message to RabbitMQ queue for processing.
        /// </summary>
        /// <param name="fileStream">The CSV file stream to import.</param>
        /// <param name="fileName">The name of the CSV file.</param>
        /// <returns>Job ID for tracking the import status.</returns>
        Task<Guid> ImportCsvAsync(Stream fileStream, string fileName);

        /// <summary>
        /// Export data as CSV file.
        /// Queries data from database, generates CSV, uploads to blob storage and returns download URL.
        /// </summary>
        /// <param name="exportFileName">The name for the exported CSV file.</param>
        /// <returns>Presigned URL for downloading the exported file.</returns>
        Task<string> ExportCsvAsync(string exportFileName);
    }
}