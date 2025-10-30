using CSVProssessor.Application.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace CSVProssessor.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CsvController : ControllerBase
    {
        public readonly ICsvService _csvService;

        public CsvController(ICsvService csvService)
        {
            _csvService = csvService;
        }

        [HttpPost("upload")]
        public async Task<IActionResult> UploadCsvAsync(IFormFile file)
        {
            if (file == null || file.Length == 0)
            {
                return BadRequest("No file uploaded.");
            }
            using var stream = file.OpenReadStream();
            var importId = await _csvService.ImportCsvAsync(stream, file.FileName);
            return Ok(new { ImportId = importId });
        }
    }
}