namespace Muflone.Transport.Kafka.Serialization;

internal sealed class KafkaEnvelope
{
    public string MessageType { get; init; } = string.Empty;
    public string Payload { get; init; } = string.Empty;
}
