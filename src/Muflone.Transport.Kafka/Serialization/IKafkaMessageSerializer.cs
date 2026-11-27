using Muflone.Messages;

namespace Muflone.Transport.Kafka.Serialization;

internal interface IKafkaMessageSerializer : IAsyncDisposable
{
    Task<byte[]> SerializeAsync<TMessage>(string topicName, TMessage message, CancellationToken cancellationToken)
        where TMessage : class, IMessage;

    Task<TMessage?> DeserializeAsync<TMessage>(string topicName, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
        where TMessage : class, IMessage;

    Task<string?> DeserializePayloadAsync(string topicName, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}
