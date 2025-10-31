namespace CSVProssessor.Domain.DTOs.CsvJobDTOs
{
    /// <summary>
    /// Message gửi vào topic "csv-changes-topic" để thông báo về thay đổi dữ liệu
    /// Tất cả instance đều sẽ nhận được message này (fan-out pattern)
    /// </summary>
    public class CsvChangeNotificationMessage
    {
        /// <summary>
        /// Loại thay đổi: "Created", "Updated", "Deleted"
        /// </summary>
        public string ChangeType { get; set; } = string.Empty;

        /// <summary>
        /// Danh sách ID của bản ghi thay đổi
        /// </summary>
        public List<Guid> RecordIds { get; set; } = new List<Guid>();

        /// <summary>
        /// Tổng số bản ghi thay đổi
        /// </summary>
        public int TotalChanges { get; set; }

        /// <summary>
        /// Thời gian phát hiện thay đổi
        /// </summary>
        public DateTime DetectedAt { get; set; }

        /// <summary>
        /// Thời gian bắt đầu kiểm tra (để tracking)
        /// </summary>
        public DateTime? CheckStartTime { get; set; }

        /// <summary>
        /// Thời gian kết thúc kiểm tra (để tracking)
        /// </summary>
        public DateTime? CheckEndTime { get; set; }

        /// <summary>
        /// Tên instance gửi message (ví dụ: "api-1", "api-2")
        /// </summary>
        public string? InstanceName { get; set; }
    }
}
