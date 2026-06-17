using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Muflone.Messages;
using Muflone.Messages.Commands;
using Muflone.Messages.Events;
using Muflone.Persistence;
using Muflone.Transport.Kafka.Models;
using Muflone.Transport.Kafka.Serialization;
using System.Text;

namespace Muflone.Transport.Kafka;

public sealed class ServiceBus : IServiceBus, IEventBus, IDisposable
{
    private readonly KafkaConfiguration _configuration;
    private readonly IKafkaMessageSerializer _kafkaMessageSerializer;
    private readonly ILogger _logger;
    private readonly object _producerLock = new();
    private IProducer<string, byte[]>? _producer;

    public ServiceBus(
        KafkaConfiguration configuration,
        ILoggerFactory loggerFactory,
        ISerializer? messageSerializer = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _kafkaMessageSerializer = KafkaMessageSerializerFactory.Create(configuration, messageSerializer ?? new Serializer());
        _logger = loggerFactory?.CreateLogger(GetType()) ?? throw new ArgumentNullException(nameof(loggerFactory));
    }

    public Task PublishAsync<T>(T @event, CancellationToken cancellationToken = default)
        where T : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(@event);
        return ProduceAsync(@event, cancellationToken);
    }

    public Task SendAsync<T>(T command, CancellationToken cancellationToken = default)
        where T : class, ICommand
    {
        ArgumentNullException.ThrowIfNull(command);
        return ProduceAsync(command, cancellationToken);
    }

    private async Task ProduceAsync<TMessage>(TMessage message, CancellationToken cancellationToken)
        where TMessage : class, IMessage
    {
        var topicName = _configuration.GetTopicName(message.GetType());
        var serializedMessage = await _kafkaMessageSerializer.SerializeAsync(topicName, message, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Publishing message '{MessageId}' of type '{MessageType}' to topic '{TopicName}'",
            message.MessageId,
            message.GetType().FullName,
            topicName);

        var kafkaMessage = new Message<string, byte[]>
        {
            Key = message.MessageId.ToString(),
            Value = serializedMessage,
            Headers = BuildHeaders(message)
        };

        await GetProducer().ProduceAsync(topicName, kafkaMessage, cancellationToken).ConfigureAwait(false);
    }

    private IProducer<string, byte[]> GetProducer()
    {
        if (_producer != null)
            return _producer;

        lock (_producerLock)
        {
            if (_producer != null)
                return _producer;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _configuration.BootstrapServers,
                ClientId = _configuration.ClientId
            };

            _configuration.ApplyBrokerAuthentication(producerConfig);

            _producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();
            return _producer;
        }
    }

    private static Headers BuildHeaders(IMessage message)
    {
        var headers = new Headers
        {
            { "message-type", Encoding.UTF8.GetBytes(message.GetType().FullName ?? message.GetType().Name) }
        };

        foreach (var userProperty in message.UserProperties)
        {
            if (userProperty.Value is null)
                continue;

            headers.Add(userProperty.Key, Encoding.UTF8.GetBytes(userProperty.Value.ToString()!));
        }

        return headers;
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(10));
        _producer?.Dispose();
        _kafkaMessageSerializer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }
}
