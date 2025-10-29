using CSVProssessor.Infrastructure.Interfaces;

namespace CSVProssessor.Infrastructure.Commons;

public class CurrentTime : ICurrentTime
{
    public DateTime GetCurrentTime()
    {
        return DateTime.UtcNow.ToUniversalTime();
    }
}