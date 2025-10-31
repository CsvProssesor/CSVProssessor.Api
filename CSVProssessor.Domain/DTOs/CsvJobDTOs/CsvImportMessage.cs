namespace CSVProssessor.Domain.DTOs.CsvJobDTOs
{
    /// <summary>
    /// Message gửi vào queue "csv-import-queue" để thông báo worker xử lý import file
    /// </summary>
    public class CsvImportMessage
    {
        /// <summary>
        /// ID của job import
        /// </summary>
        public Guid JobId { get; set; }

        /// <summary>
        /// Tên file CSV (thường kèm jobId để đảm bảo unique)
        /// </summary>
        public string FileName { get; set; } = string.Empty;

        /// <summary>
        /// Thời gian file được upload
        /// </summary>
        public DateTime UploadedAt { get; set; }
    }
}
