using CSVProssessor.Domain.Entities;
using Microsoft.EntityFrameworkCore;

namespace CSVProssessor.Domain
{
    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        public DbSet<CsvJob> CsvJobs { get; set; }
        public DbSet<CsvRecord> CsvRecords { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // CsvJob config
            modelBuilder.Entity<CsvJob>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).IsRequired();
                entity.Property(e => e.FileName).HasMaxLength(256).IsRequired();
                entity.Property(e => e.Type)
                      .HasConversion<string>() // Enum as string
                      .IsRequired();
                entity.Property(e => e.Status)
                      .HasConversion<string>() // Enum as string
                      .IsRequired();
                entity.Property(e => e.CreatedAt).IsRequired();
            });

            // CsvRecord config
            modelBuilder.Entity<CsvRecord>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.Property(e => e.Id).IsRequired();
                entity.Property(e => e.JobId).IsRequired();
                entity.Property(e => e.FileName).HasMaxLength(256).IsRequired();
                entity.Property(e => e.ImportedAt).IsRequired();
                entity.Property(e => e.UpdatedAt).IsRequired();
                entity.Property(e => e.Data)
                      .HasColumnType("jsonb")
                      .IsRequired();

                entity.HasIndex(e => e.JobId);
                entity.HasOne<CsvJob>()
                      .WithMany()
                      .HasForeignKey(e => e.JobId)
                      .OnDelete(DeleteBehavior.Cascade);
            });
        }
    }
}