using Outbox.Domain.Abstractions;
using Outbox.Domain.Orders.Events;

namespace Outbox.Domain.Orders;

/// <summary>
/// The write-model aggregate. Every state transition below raises exactly one
/// domain event via the inherited <c>Raise</c> — that is the entire developer
/// contract for producing an event. Nothing here knows the outbox exists.
/// </summary>
public sealed class Order : AggregateRoot
{
    public Guid OrderId { get; private set; }
    public Guid CustomerId { get; private set; }
    public decimal Amount { get; private set; }
    public OrderStatus Status { get; private set; }
    public int Version { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset UpdatedAt { get; private set; }

    // EF Core materialization constructor.
    private Order() { }

    public static Order Place(Guid orderId, Guid customerId, decimal amount)
    {
        if (amount <= 0)
        {
            throw new InvalidOperationException("Order amount must be positive.");
        }

        var now = DateTimeOffset.UtcNow;
        var order = new Order
        {
            OrderId = orderId,
            CustomerId = customerId,
            Amount = amount,
            Status = OrderStatus.Pending,
            Version = 1,
            CreatedAt = now,
            UpdatedAt = now,
        };

        order.Raise(new OrderPlaced(orderId, customerId, amount, now));
        return order;
    }

    public void Pay()
    {
        if (Status != OrderStatus.Pending)
        {
            throw new InvalidOperationException($"Cannot pay an order in status {Status}.");
        }

        Status = OrderStatus.Paid;
        Touch();
        Raise(new OrderPaid(OrderId, UpdatedAt));
    }

    public void Cancel(string reason)
    {
        if (Status == OrderStatus.Paid)
        {
            throw new InvalidOperationException("Cannot cancel an order that has already been paid.");
        }

        if (Status == OrderStatus.Cancelled)
        {
            throw new InvalidOperationException("Order is already cancelled.");
        }

        Status = OrderStatus.Cancelled;
        Touch();
        Raise(new OrderCancelled(OrderId, reason, UpdatedAt));
    }

    private void Touch()
    {
        Version += 1;
        UpdatedAt = DateTimeOffset.UtcNow;
    }
}
