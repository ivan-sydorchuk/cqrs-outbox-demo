using MediatR;

namespace Outbox.App.Cqrs;

/// <summary>Pure read. No handler for this may mutate anything or raise a domain event.</summary>
public interface IQuery<out TResponse> : IRequest<TResponse>;
