using Microsoft.Extensions.Logging;
using Muflone.Messages.Events;
using Muflone.Persistence;
using Muflone.Transport.Kafka.Models;

namespace Muflone.Transport.Kafka.Consumers;

public abstract class IntegrationEventsConsumerBase<T>(
    KafkaConfiguration configuration,
    ILoggerFactory loggerFactory,
    ISerializer? messageSerializer = null)
    : ConsumerBase(configuration, loggerFactory, messageSerializer), IIntegrationEventConsumer<T>
    where T : IntegrationEvent
{
    protected abstract IEnumerable<IIntegrationEventHandlerAsync<T>> HandlersAsync { get; }

    public async Task ConsumeAsync(T message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        foreach (var handlerAsync in HandlersAsync)
            await handlerAsync.HandleAsync(message, cancellationToken).ConfigureAwait(false);
    }

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        return StartConsumerAsync<T>(ConsumeAsync, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        return StopConsumerAsync(cancellationToken);
    }
}

