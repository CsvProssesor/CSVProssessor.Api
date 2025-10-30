namespace CSVProssessor.Application.Interfaces
{
    public interface ICsvService
    {
        //upload
        Task<Guid> ImportCsvAsync(Stream fileStream, string fileName);

        //download
        Task<string> ExportCsvAsync(string exportFileName);
    }
}