using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Muflone.Messages;
using Muflone.Persistence;
using Muflone.Transport.Kafka.Models;
using Muflone.Transport.Kafka.Serialization;
using KafkaConsumerClient = Confluent.Kafka.IConsumer<string, byte[]>;

namespace Muflone.Transport.Kafka.Consumers;


public class KafkaSubscriber(
    ILoggerFactory loggerFactory,
    IServiceProvider serviceProvider,
    KafkaConfiguration configuration) : MessageSubscriberBase<KafkaSubscriptionChannel>(loggerFactory, serviceProvider)
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<KafkaSubscriber>();
    private readonly IKafkaMessageSerializer _kafkaMessageSerializer = KafkaMessageSerializerFactory.Create(configuration, new Serializer());

    protected override async Task StopChannelAsync(HandlerSubscription<KafkaSubscriptionChannel> handlerSubscription)
    {
        if (handlerSubscription.Channel is null)
            return;

        await handlerSubscription.Channel.StopAsync();
        handlerSubscription.Channel = null;
    }

    protected override Task InitChannelAsync(HandlerSubscription<KafkaSubscriptionChannel> handlerSubscription)
    {
        var groupId = GetGroupId(handlerSubscription);
        var topicName = GetTopicName(handlerSubscription);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = configuration.BootstrapServers,
            GroupId = groupId,
            ClientId = $"{configuration.ClientId}.{handlerSubscription.EventTypeName}",
            AutoOffsetReset = configuration.AutoOffsetReset,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = true
        };

        configuration.ApplyBrokerAuthentication(consumerConfig);

        var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(topicName);

        _logger.LogInformation(
            "Initialized Kafka handler subscription for topic '{TopicName}' with group '{GroupId}'",
            topicName,
            groupId);

        handlerSubscription.Channel = new KafkaSubscriptionChannel(consumer);
        return Task.CompletedTask;
    }

    protected override Task InitSubscriptionAsync(HandlerSubscription<KafkaSubscriptionChannel> handlerSubscription)
    {
        var channel = handlerSubscription.Channel
            ?? throw new InvalidOperationException("Kafka channel has not been initialized.");

        if (channel.ConsumerTask is not null)
            return Task.CompletedTask;

        channel.ConsumerTask = Task.Run(async () =>
        {
            while (!channel.StoppingTokenSource.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? consumeResult;

                try
                {
                    consumeResult = channel.Consumer.Consume(channel.StoppingTokenSource.Token);
                }
                catch (OperationCanceledException) when (channel.StoppingTokenSource.IsCancellationRequested)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consume error for handler '{EventTypeName}': {Reason}", handlerSubscription.EventTypeName, ex.Error.Reason);
                    continue;
                }

                if (consumeResult?.Message?.Value is null)
                    continue;

                try
                {
                    var payload = await _kafkaMessageSerializer
                        .DeserializePayloadAsync(consumeResult.Topic, consumeResult.Message.Value, channel.StoppingTokenSource.Token);
                        

                    if (payload is null)
                        continue;

                    await handlerSubscription.MessageAsync(payload, CancellationToken.None);
                    channel.Consumer.Commit(consumeResult);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "An error occurred while dispatching Kafka message for '{EventTypeName}' from topic '{TopicName}'",
                        handlerSubscription.EventTypeName,
                        consumeResult.Topic);
                }
            }
        });

        return Task.CompletedTask;
    }

    private static string GetTopicName(HandlerSubscription<KafkaSubscriptionChannel> handlerSubscription)
    => handlerSubscription.EventTypeName.ToLowerInvariant();


    // topicHandler-{Guid}
    // private string GetGroupId(HandlerSubscription<KafkaSubscriptionChannel> handlerSubscription)
    // {
    //     var groupId = $"{configuration.GroupId}.{handlerSubscription.EventTypeName}";
    //     if (groupId.EndsWith("Consumer", StringComparison.InvariantCultureIgnoreCase))
    //         groupId = groupId[..^"Consumer".Length];
    //
    //     if (!handlerSubscription.IsCommandHandler || !handlerSubscription.IsSingletonHandler)
    //         groupId = $"{groupId}.{handlerSubscription.HandlerSubscriptionId}";
    //
    //     return groupId;
    // }

    // topicHandler
    private string GetGroupId(HandlerSubscription<KafkaSubscriptionChannel> handlerSubscription)
    {
        var groupId = $"{configuration.GroupId}.{handlerSubscription.EventTypeName}";

        if (groupId.EndsWith("Consumer", StringComparison.InvariantCultureIgnoreCase))
            groupId = groupId[..^"Consumer".Length];

        if (handlerSubscription.Configuration?.InstanceId is { Length: > 0 } instanceId)
            groupId = $"{groupId}.{instanceId}";

        return groupId;
    }
}

public sealed class KafkaSubscriptionChannel(KafkaConsumerClient consumer)
{
    public KafkaConsumerClient Consumer { get; } = consumer;
    public CancellationTokenSource StoppingTokenSource { get; } = new();
    public Task? ConsumerTask { get; set; }

    public async Task StopAsync()
    {
        await StoppingTokenSource.CancelAsync();

        if (ConsumerTask is not null)
        {
            try
            {
                await ConsumerTask;
            }
            catch (OperationCanceledException)
            {
            }
        }

        Consumer.Close();
        Consumer.Dispose();
        StoppingTokenSource.Dispose();
    }
}
