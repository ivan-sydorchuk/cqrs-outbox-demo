using MediatR;
using Outbox.Infrastructure;

namespace Outbox.App.Cqrs;

/// <summary>
/// The other half of the "magic". Every command handler in this project loads
/// an aggregate, calls a domain method on it, and returns — it never calls
/// SaveChanges and never touches the outbox. This behavior runs the handler,
/// then calls <see cref="AppDbContext.SaveChangesAsync"/> exactly once, which
/// is what actually persists both the aggregate change and the outbox rows
/// derived from it, atomically. Query handlers don't implement ICommandBase,
/// so this behavior never runs for them.
/// </summary>
public sealed class UnitOfWorkBehavior<TRequest, TResponse>(AppDbContext db)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : ICommandBase
{
    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken)
    {
        var response = await next();
        await db.SaveChangesAsync(cancellationToken);
        return response;
    }
}
