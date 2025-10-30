namespace CSVProssessor.Domain.Enums
{
    public enum CsvJobType
    {
        Import = 0,
        Export = 1
    }

    public enum CsvJobStatus
    {
        Pending = 0,      // Đang chờ xử lý
        Processing = 1,   // Đang xử lý
        Completed = 2,    // Đã hoàn thành
        Failed = 3        // Lỗi
    }
}