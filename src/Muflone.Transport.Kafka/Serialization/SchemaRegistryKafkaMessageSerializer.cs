using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Muflone.Persistence;
using Muflone.Transport.Kafka.Models;
using Muflone.Transport.Kafka.Serialization.Protobuf;
using JsonDeserializer = Confluent.SchemaRegistry.Serdes.JsonDeserializer<Muflone.Transport.Kafka.Serialization.KafkaEnvelope>;
using JsonSerializer = Confluent.SchemaRegistry.Serdes.JsonSerializer<Muflone.Transport.Kafka.Serialization.KafkaEnvelope>;
using ProtobufDeserializer = Confluent.SchemaRegistry.Serdes.ProtobufDeserializer<Muflone.Transport.Kafka.Serialization.Protobuf.KafkaEnvelopeContract>;
using ProtobufSerializer = Confluent.SchemaRegistry.Serdes.ProtobufSerializer<Muflone.Transport.Kafka.Serialization.Protobuf.KafkaEnvelopeContract>;

namespace Muflone.Transport.Kafka.Serialization;

internal static class KafkaMessageSerializerFactory
{
    public static IKafkaMessageSerializer Create(KafkaConfiguration configuration, ISerializer messageSerializer)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(messageSerializer);

        return configuration.SerializationFormat switch
        {
            KafkaSerializationFormat.Legacy => new LegacyKafkaMessageSerializer(messageSerializer),
            KafkaSerializationFormat.Json or KafkaSerializationFormat.Avro or KafkaSerializationFormat.Protobuf
                => new SchemaRegistryKafkaMessageSerializer(configuration, messageSerializer),
            _ => throw new NotSupportedException($"Serialization format '{configuration.SerializationFormat}' is not supported.")
        };
    }
}

internal sealed class SchemaRegistryKafkaMessageSerializer : IKafkaMessageSerializer
{
    private static readonly Avro.Schema AvroEnvelopeSchema = Avro.Schema.Parse("""
        {
          "type": "record",
          "name": "KafkaEnvelope",
          "namespace": "Muflone.Transport.Kafka.Serialization",
          "fields": [
            { "name": "messageType", "type": "string" },
            { "name": "payload", "type": "string" }
          ]
        }
        """);

    private readonly KafkaConfiguration _configuration;
    private readonly ISerializer _messageSerializer;
    private readonly CachedSchemaRegistryClient _schemaRegistryClient;
    private readonly JsonSerializer? _jsonSerializer;
    private readonly JsonDeserializer? _jsonDeserializer;
    private readonly AvroSerializer<GenericRecord>? _avroSerializer;
    private readonly AvroDeserializer<GenericRecord>? _avroDeserializer;
    private readonly ProtobufSerializer? _protobufSerializer;
    private readonly ProtobufDeserializer? _protobufDeserializer;

    public SchemaRegistryKafkaMessageSerializer(KafkaConfiguration configuration, ISerializer messageSerializer)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _messageSerializer = messageSerializer ?? throw new ArgumentNullException(nameof(messageSerializer));

        _schemaRegistryClient = new CachedSchemaRegistryClient(configuration.ToSchemaRegistryConfig());

        switch (_configuration.SerializationFormat)
        {
            case KafkaSerializationFormat.Json:
                _jsonSerializer = new JsonSerializer(
                    _schemaRegistryClient,
                    new JsonSerializerConfig
                    {
                        AutoRegisterSchemas = _configuration.SchemaRegistryAutoRegisterSchemas
                    });
                _jsonDeserializer = new JsonDeserializer(_schemaRegistryClient);
                break;
            case KafkaSerializationFormat.Avro:
                _avroSerializer = new AvroSerializer<GenericRecord>(
                    _schemaRegistryClient,
                    new AvroSerializerConfig
                    {
                        AutoRegisterSchemas = _configuration.SchemaRegistryAutoRegisterSchemas
                    });
                _avroDeserializer = new AvroDeserializer<GenericRecord>(_schemaRegistryClient);
                break;
            case KafkaSerializationFormat.Protobuf:
                _protobufSerializer = new ProtobufSerializer(
                    _schemaRegistryClient,
                    new ProtobufSerializerConfig
                    {
                        AutoRegisterSchemas = _configuration.SchemaRegistryAutoRegisterSchemas
                    });
                _protobufDeserializer = new ProtobufDeserializer(_schemaRegistryClient);
                break;
            default:
                throw new NotSupportedException($"Serialization format '{_configuration.SerializationFormat}' is not supported.");
        }
    }

    public async Task<byte[]> SerializeAsync<TMessage>(string topicName, TMessage message, CancellationToken cancellationToken)
        where TMessage : class, Muflone.Messages.IMessage
    {
        ArgumentNullException.ThrowIfNull(message);

        var payload = await _messageSerializer.SerializeAsync(message, cancellationToken);
        var envelope = new KafkaEnvelope
        {
            MessageType = message.GetType().AssemblyQualifiedName ?? message.GetType().FullName ?? message.GetType().Name,
            Payload = payload
        };

        var context = CreateSerializationContext(topicName);

        return _configuration.SerializationFormat switch
        {
            KafkaSerializationFormat.Json => await _jsonSerializer!.SerializeAsync(envelope, context),
            KafkaSerializationFormat.Avro => await _avroSerializer!.SerializeAsync(ToAvroEnvelope(envelope), context),
            KafkaSerializationFormat.Protobuf => await _protobufSerializer!.SerializeAsync(ToProtobufEnvelope(envelope), context),
            _ => throw new NotSupportedException($"Serialization format '{_configuration.SerializationFormat}' is not supported.")
        };
    }

    public async Task<TMessage?> DeserializeAsync<TMessage>(string topicName, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
        where TMessage : class, Muflone.Messages.IMessage
    {
        var envelope = await DeserializeEnvelopeAsync(topicName, payload).ConfigureAwait(false);
        if (envelope == null)
            return default;

        return await _messageSerializer.DeserializeAsync<TMessage>(envelope.Payload, cancellationToken).ConfigureAwait(false);
    }

    public async Task<string?> DeserializePayloadAsync(string topicName, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var envelope = await DeserializeEnvelopeAsync(topicName, payload).ConfigureAwait(false);
        return envelope?.Payload;
    }

    public async ValueTask DisposeAsync()
    {
        switch (_jsonSerializer)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }

        switch (_jsonDeserializer)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }

        _schemaRegistryClient.Dispose();
    }

    private async Task<KafkaEnvelope?> DeserializeEnvelopeAsync(string topicName, ReadOnlyMemory<byte> payload)
    {
        var context = CreateSerializationContext(topicName);

        return _configuration.SerializationFormat switch
        {
            KafkaSerializationFormat.Json => await _jsonDeserializer!.DeserializeAsync(payload, false, context).ConfigureAwait(false),
            KafkaSerializationFormat.Avro => FromAvroEnvelope(await _avroDeserializer!.DeserializeAsync(payload, false, context).ConfigureAwait(false)),
            KafkaSerializationFormat.Protobuf => FromProtobufEnvelope(await _protobufDeserializer!.DeserializeAsync(payload, false, context).ConfigureAwait(false)),
            _ => throw new NotSupportedException($"Serialization format '{_configuration.SerializationFormat}' is not supported.")
        };
    }

    private static SerializationContext CreateSerializationContext(string topicName)
    {
        return new SerializationContext(MessageComponentType.Value, topicName);
    }

    private static GenericRecord ToAvroEnvelope(KafkaEnvelope envelope)
    {
        var record = new GenericRecord((RecordSchema)AvroEnvelopeSchema);
        record.Add("messageType", envelope.MessageType);
        record.Add("payload", envelope.Payload);
        return record;
    }

    private static KafkaEnvelope? FromAvroEnvelope(GenericRecord? envelope)
    {
        if (envelope == null)
            return null;

        return new KafkaEnvelope
        {
            MessageType = envelope["messageType"]?.ToString() ?? string.Empty,
            Payload = envelope["payload"]?.ToString() ?? string.Empty
        };
    }

    private static KafkaEnvelopeContract ToProtobufEnvelope(KafkaEnvelope envelope)
    {
        return new KafkaEnvelopeContract
        {
            MessageType = envelope.MessageType,
            Payload = envelope.Payload
        };
    }

    private static KafkaEnvelope? FromProtobufEnvelope(KafkaEnvelopeContract? envelope)
    {
        if (envelope == null)
            return null;

        return new KafkaEnvelope
        {
            MessageType = envelope.MessageType,
            Payload = envelope.Payload
        };
    }
}
