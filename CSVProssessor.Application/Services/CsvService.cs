using CSVProssessor.Application.Interfaces;
using CSVProssessor.Infrastructure.Interfaces;

namespace CSVProssessor.Application.Services
{
    public class CsvService : ICsvService
    {
        public readonly IUnitOfWork _unitOfWork;

        public CsvService(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }
    }
}
