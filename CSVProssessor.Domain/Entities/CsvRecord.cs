using System.Text.Json.Nodes;

namespace CSVProssessor.Domain.Entities
{
    public class CsvRecord : BaseEntity
    {
        public Guid JobId { get; set; }
        public string FileName { get; set; }
        public DateTime ImportedAt { get; set; }
        public JsonObject Data { get; set; } // Dữ liệu động từ CSV
    }
}
