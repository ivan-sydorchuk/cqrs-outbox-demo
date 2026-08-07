using Outbox.App.Cqrs;
using Outbox.Infrastructure;

namespace Outbox.App.Orders;

public sealed record PayOrderCommand(Guid OrderId) : ICommand;

public sealed class PayOrderCommandHandler(AppDbContext db) : ICommandHandler<PayOrderCommand>
{
    public async Task Handle(PayOrderCommand request, CancellationToken cancellationToken)
    {
        var order = await db.Orders.FindAsync([request.OrderId], cancellationToken)
            ?? throw new InvalidOperationException($"Order {request.OrderId} not found.");

        order.Pay();
    }
}
