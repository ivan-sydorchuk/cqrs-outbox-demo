using Outbox.App.Cqrs;
using Outbox.Domain.Orders;
using Outbox.Infrastructure;

namespace Outbox.App.Orders;

public sealed record PlaceOrderCommand(Guid OrderId, Guid CustomerId, decimal Amount) : ICommand<Guid>;

/// <summary>
/// This is the entire "developer contract" from the practice task in one file:
/// load/create the aggregate, call a domain method, hand it to the DbSet.
/// There is no event-persistence code here — UnitOfWorkBehavior does that
/// after this handler returns.
/// </summary>
public sealed class PlaceOrderCommandHandler(AppDbContext db) : ICommandHandler<PlaceOrderCommand, Guid>
{
    public Task<Guid> Handle(PlaceOrderCommand request, CancellationToken cancellationToken)
    {
        var order = Order.Place(request.OrderId, request.CustomerId, request.Amount);
        db.Orders.Add(order);
        return Task.FromResult(order.OrderId);
    }
}
