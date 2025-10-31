using CSVProssessor.Application.Utils;
using CSVProssessor.Domain;
using CSVProssessor.Infrastructure.Interfaces;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;


[ApiController]
[Route("api/system")]
public class SystemController : ControllerBase
{
    private readonly AppDbContext _context;
    private readonly ILoggerService _logger;


    public SystemController(AppDbContext context, ILoggerService logger)
    {
        _context = context;
        _logger = logger;
    }



    [HttpDelete("database")]
    public async Task<IActionResult> ClearDb()
    {
        try
        {
            await ClearDatabase(_context);

            return Ok(ApiResult<object>.Success(new
            {
                Message = "Data cleard successfully."
            }));
        }
        catch (Exception ex)
        {
            var statusCode = ExceptionUtils.ExtractStatusCode(ex);
            var errorResponse = ExceptionUtils.CreateErrorResponse<object>(ex);
            return StatusCode(statusCode, errorResponse);
        }
    }

    private async Task ClearDatabase(AppDbContext context)
    {
        var strategy = context.Database.CreateExecutionStrategy();

        await strategy.ExecuteAsync(async () =>
        {
            await using var transaction = await context.Database.BeginTransactionAsync();

            try
            {
                _logger.Info("Bắt đầu xóa dữ liệu trong database...");

                var tablesToDelete = new List<Func<Task>>
                {
                   () => context.CsvJobs.ExecuteDeleteAsync(),
                   () => context.CsvRecords.ExecuteDeleteAsync(),

                };

                foreach (var deleteFunc in tablesToDelete)
                    await deleteFunc();

                await transaction.CommitAsync();

                _logger.Success("Xóa sạch dữ liệu trong database thành công.");
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                _logger.Error($"Xóa dữ liệu thất bại: {ex.Message}");
                throw;
            }
        });
    }


}