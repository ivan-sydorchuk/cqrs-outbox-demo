using Microsoft.EntityFrameworkCore;
using Outbox.App.Cqrs;
using Outbox.Domain.Orders;
using Outbox.Infrastructure;

namespace Outbox.App.Orders;

public sealed record OrderDto(Guid OrderId, Guid CustomerId, decimal Amount, OrderStatus Status, int Version);

public sealed record GetOrderByIdQuery(Guid OrderId) : IQuery<OrderDto?>;

/// <summary>
/// Read side of CQS: no state changes, so AsNoTracking is always safe here —
/// this handler could never accidentally flush a write, because it never
/// implements ICommandBase and therefore never routes through
/// UnitOfWorkBehavior at all.
/// </summary>
public sealed class GetOrderByIdQueryHandler(AppDbContext db) : IQueryHandler<GetOrderByIdQuery, OrderDto?>
{
    public Task<OrderDto?> Handle(GetOrderByIdQuery request, CancellationToken cancellationToken) =>
        db.Orders
            .AsNoTracking()
            .Where(o => o.OrderId == request.OrderId)
            .Select(o => new OrderDto(o.OrderId, o.CustomerId, o.Amount, o.Status, o.Version))
            .FirstOrDefaultAsync(cancellationToken);
}
