using MediatR;

namespace Outbox.App.Cqrs;

// Thin naming wrappers over MediatR's IRequestHandler so handler classes read
// as "command handler" / "query handler" rather than generic "request handler".
public interface ICommandHandler<in TCommand, TResponse> : IRequestHandler<TCommand, TResponse>
    where TCommand : ICommand<TResponse>;

public interface ICommandHandler<in TCommand> : IRequestHandler<TCommand>
    where TCommand : ICommand;

public interface IQueryHandler<in TQuery, TResponse> : IRequestHandler<TQuery, TResponse>
    where TQuery : IQuery<TResponse>;
