using Microsoft.Extensions.Logging;
using Muflone.Messages.Commands;
using Muflone.Persistence;
using Muflone.Transport.Kafka.Models;

namespace Muflone.Transport.Kafka.Consumers;

public abstract class CommandConsumerBase<T>(
    IRepository repository,
    KafkaConfiguration configuration,
    ILoggerFactory loggerFactory,
    ISerializer? messageSerializer = null) : ConsumerBase(configuration, loggerFactory, messageSerializer), ICommandConsumer<T>
    where T : Command
{
    protected abstract ICommandHandlerAsync<T> HandlerAsync { get; }
    protected IRepository Repository { get; } = repository ?? throw new ArgumentNullException(nameof(repository));

    public Task ConsumeAsync(T message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        return HandlerAsync.HandleAsync(message, cancellationToken);
    }

    public Task StartAsync(CancellationToken cancellationToken = default) =>
        StartConsumerAsync<T>(ConsumeAsync, cancellationToken);
   

    public Task StopAsync(CancellationToken cancellationToken = default) =>
        StopConsumerAsync(cancellationToken);
    
}
