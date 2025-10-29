using CSVProssessor.Domain;
using CSVProssessor.Infrastructure.Interfaces;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq.Expressions;

namespace CSVProssessor.Infrastructure;

public class UnitOfWork : IUnitOfWork
{
    private readonly AppDbContext _dbContext;
    private IDbContextTransaction? _transaction;

    public UnitOfWork(AppDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public void Dispose()
    {
        _dbContext.Dispose();
    }

    public async Task<int> SaveChangesAsync()
    {
        return await _dbContext.SaveChangesAsync();
    }

    // Where
    public IQueryable<T> Where<T>(Expression<Func<T, bool>> predicate) where T : class
    {
        return _dbContext.Set<T>().Where(predicate);
    }

    // Select
    public IQueryable<TResult> Select<T, TResult>(Expression<Func<T, TResult>> selector) where T : class
    {
        return _dbContext.Set<T>().Select(selector);
    }

    // Transaction support
    public async Task BeginTransactionAsync()
    {
        if (_transaction != null) return;
        _transaction = await _dbContext.Database.BeginTransactionAsync();
    }

    public async Task CommitAsync()
    {
        try
        {
            if (_transaction != null)
            {
                await _dbContext.SaveChangesAsync();
                await _transaction.CommitAsync();
            }
        }
        finally
        {
            await DisposeTransactionAsync();
        }
    }

    public async Task RollbackAsync()
    {
        try
        {
            if (_transaction != null)
                await _transaction.RollbackAsync();
        }
        finally
        {
            await DisposeTransactionAsync();
        }
    }

    private async Task DisposeTransactionAsync()
    {
        if (_transaction != null)
        {
            await _transaction.DisposeAsync();
            _transaction = null;
        }
    }
}