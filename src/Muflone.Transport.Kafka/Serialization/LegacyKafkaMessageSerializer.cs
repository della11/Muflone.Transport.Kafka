using System.Text;
using Muflone.Messages;
using Muflone.Persistence;

namespace Muflone.Transport.Kafka.Serialization;

internal sealed class LegacyKafkaMessageSerializer(ISerializer messageSerializer) : IKafkaMessageSerializer
{
    public async Task<byte[]> SerializeAsync<TMessage>(string topicName, TMessage message, CancellationToken cancellationToken)
        where TMessage : class, IMessage
    {
        ArgumentNullException.ThrowIfNull(message);

        var serializedMessage = await messageSerializer.SerializeAsync(message, cancellationToken);
        return Encoding.UTF8.GetBytes(serializedMessage);
    }

    public async Task<TMessage?> DeserializeAsync<TMessage>(string topicName, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
        where TMessage : class, IMessage
    {
        var serializedMessage = Encoding.UTF8.GetString(payload.Span);
        return await messageSerializer.DeserializeAsync<TMessage>(serializedMessage, cancellationToken);
    }

    public Task<string?> DeserializePayloadAsync(string topicName, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var serializedMessage = Encoding.UTF8.GetString(payload.Span);
        return Task.FromResult<string?>(serializedMessage);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
