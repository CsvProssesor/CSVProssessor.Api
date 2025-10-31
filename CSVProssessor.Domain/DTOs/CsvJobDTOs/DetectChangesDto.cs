namespace CSVProssessor.Domain.DTOs.CsvJobDTOs
{
    /// <summary>
    /// Request DTO for DetectAndPublishChangesAsync
    /// Contains parameters for detecting changes and publishing notifications
    /// </summary>
    public class DetectChangesRequestDto
    {
        /// <summary>
        /// Last time the check was performed. If null, look back X minutes instead.
        /// </summary>
        public DateTime? LastCheckTime { get; set; }

        /// <summary>
        /// Number of minutes to look back (default 5). Used only if LastCheckTime is null.
        /// </summary>
        public int MinutesBack { get; set; } = 5;

        /// <summary>
        /// Type of change: "Created", "Updated", etc. (default "Created")
        /// </summary>
        public string ChangeType { get; set; } = "Created";

        /// <summary>
        /// Name of the instance sending the message (e.g., "api-1", "api-2")
        /// </summary>
        public string? InstanceName { get; set; }

        /// <summary>
        /// Whether to publish change notification to topic (default true)
        /// </summary>
        public bool PublishToTopic { get; set; } = true;
    }

    /// <summary>
    /// Response DTO for DetectAndPublishChangesAsync
    /// Contains information about detected changes and publishing status
    /// </summary>
    public class DetectChangesResponseDto
    {
        /// <summary>
        /// List of detected changes
        /// </summary>
        public List<CsvRecordDto> Changes { get; set; } = new List<CsvRecordDto>();

        /// <summary>
        /// Total number of changes detected
        /// </summary>
        public int TotalChanges { get; set; }

        /// <summary>
        /// Whether changes were published to topic
        /// </summary>
        public bool PublishedToTopic { get; set; }

        /// <summary>
        /// Timestamp when changes were detected
        /// </summary>
        public DateTime DetectedAt { get; set; }

        /// <summary>
        /// Check period start time
        /// </summary>
        public DateTime CheckStartTime { get; set; }

        /// <summary>
        /// Check period end time
        /// </summary>
        public DateTime CheckEndTime { get; set; }

        /// <summary>
        /// Success message
        /// </summary>
        public string Message { get; set; } = string.Empty;
    }

    /// <summary>
    /// Simple DTO for CsvRecord (for response purposes)
    /// </summary>
    public class CsvRecordDto
    {
        /// <summary>
        /// Record ID
        /// </summary>
        public Guid Id { get; set; }

        /// <summary>
        /// Job ID that this record belongs to
        /// </summary>
        public Guid JobId { get; set; }

        /// <summary>
        /// Name of the CSV file
        /// </summary>
        public string? FileName { get; set; }

        /// <summary>
        /// Import timestamp
        /// </summary>
        public DateTime ImportedAt { get; set; }

        /// <summary>
        /// Creation timestamp
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Last update timestamp
        /// </summary>
        public DateTime? UpdatedAt { get; set; }
    }
}
