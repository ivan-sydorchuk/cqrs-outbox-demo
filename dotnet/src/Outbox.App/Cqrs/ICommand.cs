using MediatR;

namespace Outbox.App.Cqrs;

/// <summary>
/// Non-generic marker so the pipeline behavior can constrain
/// IPipelineBehavior&lt;TRequest,TResponse&gt; to "commands only" without a
/// generic parameter of its own. Queries never implement this, so they never
/// pass through the outbox-saving pipeline step below (CQS: reads make zero
/// writes, so they have nothing for the behavior to save).
/// </summary>
public interface ICommandBase;

public interface ICommand : IRequest, ICommandBase;

public interface ICommand<out TResponse> : IRequest<TResponse>, ICommandBase;
