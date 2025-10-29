using System.Linq.Expressions;

namespace CSVProssessor.Infrastructure.Interfaces;

public interface IUnitOfWork : IDisposable
{
    Task<int> SaveChangesAsync();

    IQueryable<T> Where<T>(Expression<Func<T, bool>> predicate) where T : class;

    IQueryable<TResult> Select<T, TResult>(Expression<Func<T, TResult>> selector) where T : class;
}