using CSVProssessor.Infrastructure.Interfaces;
using CSVProssessor.Infrastructure.Utils;
using Microsoft.AspNetCore.Http;
using System.Security.Claims;

namespace CSVProssessor.Infrastructure.Commons;

public class ClaimsService : IClaimsService
{
    public ClaimsService(IHttpContextAccessor httpContextAccessor)
    {
        // Lấy ClaimsIdentity
        var identity = httpContextAccessor.HttpContext?.User?.Identity as ClaimsIdentity;

        var extractedId = AuthenTools.GetCurrentUserId(identity);
        if (Guid.TryParse(extractedId, out var parsedId))
            CurrentUserId = parsedId;
        else
            CurrentUserId = Guid.Empty;

        IpAddress = httpContextAccessor?.HttpContext?.Connection?.RemoteIpAddress?.ToString();
    }

    public Guid CurrentUserId { get; }

    public string? IpAddress { get; }
}