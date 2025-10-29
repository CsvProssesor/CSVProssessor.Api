using Microsoft.EntityFrameworkCore;

namespace CSVProssessor.Domain
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        // Define DbSets for your entities here
    }
}