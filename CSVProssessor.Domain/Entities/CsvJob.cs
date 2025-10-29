using CSVProssessor.Domain.Enums;

namespace CSVProssessor.Domain.Entities
{
    public class CsvJob : BaseEntity
    {
        public string FileName { get; set; }
        public CsvJobType Type { get; set; }
        public CsvJobStatus Status { get; set; }
    }
}
