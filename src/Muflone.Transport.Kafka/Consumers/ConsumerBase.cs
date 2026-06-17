using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Muflone.Messages;
using Muflone.Persistence;
using Muflone.Transport.Kafka.Models;
using Muflone.Transport.Kafka.Serialization;

namespace Muflone.Transport.Kafka.Consumers;

public abstract class ConsumerBase : IAsyncDisposable
{
    protected readonly KafkaConfiguration Configuration;
    protected readonly ILogger Logger;
    private readonly IKafkaMessageSerializer _kafkaMessageSerializer;

    private IConsumer<string, byte[]>? _consumer;
    private Task? _consumerTask;
    private CancellationTokenSource? _stoppingTokenSource;

    protected ConsumerBase(
        KafkaConfiguration configuration,
        ILoggerFactory loggerFactory,
        ISerializer? messageSerializer = null)
    {
        Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        Logger = loggerFactory?.CreateLogger(GetType()) ?? throw new ArgumentNullException(nameof(loggerFactory));
        _kafkaMessageSerializer = KafkaMessageSerializerFactory.Create(configuration, messageSerializer ?? new Serializer());
    }

    protected Task StartConsumerAsync<TMessage>(
        Func<TMessage, CancellationToken, Task> messageHandler,
        CancellationToken cancellationToken = default)
        where TMessage : class, IMessage
    {
        if (_consumerTask != null)
            return Task.CompletedTask;

        var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var consumer = CreateConsumer<TMessage>();
        var topicName = Configuration.GetTopicName(typeof(TMessage));

        consumer.Subscribe(topicName);
        Logger.LogInformation(
            "Subscribed consumer '{ConsumerType}' to topic '{TopicName}' with group '{GroupId}'",
            GetType().FullName,
            topicName,
            consumer.MemberId);

        _consumer = consumer;
        _stoppingTokenSource = linkedTokenSource;
        _consumerTask = Task.Run(() => ConsumeLoopAsync(consumer, messageHandler, linkedTokenSource.Token), CancellationToken.None);

        return Task.CompletedTask;
    }

    protected async Task StopConsumerAsync(CancellationToken cancellationToken = default)
    {
        if (_stoppingTokenSource == null || _consumerTask == null)
            return;

        await _stoppingTokenSource.CancelAsync().ConfigureAwait(false);

        try
        {
            await _consumerTask.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            _consumerTask = null;
            _stoppingTokenSource.Dispose();
            _stoppingTokenSource = null;
            _consumer = null;
        }
    }

    private IConsumer<string, byte[]> CreateConsumer<TMessage>()
        where TMessage : class, IMessage
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = Configuration.BootstrapServers,
            GroupId = Configuration.GroupId,
            ClientId = $"{Configuration.ClientId}.{typeof(TMessage).Name}",
            AutoOffsetReset = Configuration.AutoOffsetReset,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = true
        };

        Configuration.ApplyBrokerAuthentication(consumerConfig);

        return new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
    }

    private async Task ConsumeLoopAsync<TMessage>(
        IConsumer<string, byte[]> consumer,
        Func<TMessage, CancellationToken, Task> messageHandler,
        CancellationToken cancellationToken)
        where TMessage : class, IMessage
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? consumeResult;

                try
                {
                    consumeResult = consumer.Consume(cancellationToken);
                }
                catch (ConsumeException ex)
                {
                    Logger.LogError(ex, "Kafka consume error for consumer '{ConsumerType}': {ErrorReason}", GetType().FullName, ex.Error.Reason);
                    continue;
                }

                if (consumeResult?.Message?.Value == null)
                    continue;

                try
                {
                    var message = await _kafkaMessageSerializer
                        .DeserializeAsync<TMessage>(consumeResult.Topic, consumeResult.Message.Value, cancellationToken)
                        .ConfigureAwait(false);

                    if (message == null)
                    {
                        Logger.LogWarning(
                            "Kafka message at topic '{TopicName}' and offset '{Offset}' could not be deserialized as '{MessageType}'",
                            consumeResult.Topic,
                            consumeResult.Offset.Value,
                            typeof(TMessage).FullName);
                        continue;
                    }

                    Logger.LogInformation(
                        "Received message '{MessageId}' from topic '{TopicName}' at offset '{Offset}'",
                        message.MessageId,
                        consumeResult.Topic,
                        consumeResult.Offset.Value);

                    await messageHandler(message, cancellationToken).ConfigureAwait(false);
                    consumer.Commit(consumeResult);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Logger.LogError(
                        ex,
                        "An error occurred while processing a Kafka message from topic '{TopicName}' at offset '{Offset}'",
                        consumeResult.Topic,
                        consumeResult.Offset.Value);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
            consumer.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopConsumerAsync().ConfigureAwait(false);
        await _kafkaMessageSerializer.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
