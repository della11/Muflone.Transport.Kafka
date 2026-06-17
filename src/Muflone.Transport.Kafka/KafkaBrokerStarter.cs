using Microsoft.Extensions.Hosting;

namespace Muflone.Transport.Kafka;

public class KafkaBrokerStarter(IEnumerable<IConsumer> consumers) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var consumer in consumers)
            await consumer.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    
}
