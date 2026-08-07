using Outbox.App.Cqrs;
using Outbox.Infrastructure;

namespace Outbox.App.Orders;

public sealed record CancelOrderCommand(Guid OrderId, string Reason) : ICommand;

public sealed class CancelOrderCommandHandler(AppDbContext db) : ICommandHandler<CancelOrderCommand>
{
    public async Task Handle(CancelOrderCommand request, CancellationToken cancellationToken)
    {
        var order = await db.Orders.FindAsync([request.OrderId], cancellationToken)
            ?? throw new InvalidOperationException($"Order {request.OrderId} not found.");

        order.Cancel(request.Reason);
    }
}
