using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System.Globalization;

namespace Muflone.Transport.Kafka.Models;

public enum KafkaSerializationFormat
{
    Legacy = 0,
    Json = 1,
    Avro = 2,
    Protobuf = 3
}

public class KafkaConfiguration
{
    public string BootstrapServers { get; }
    public string GroupId { get; }
    public string ClientId { get; }
    public AutoOffsetReset AutoOffsetReset { get; }
    public KafkaSerializationFormat SerializationFormat { get; set; } = KafkaSerializationFormat.Legacy;
    public SecurityProtocol? BrokerSecurityProtocol { get; set; }
    public SaslMechanism? BrokerSaslMechanism { get; set; }
    public string? BrokerUsername { get; set; }
    public string? BrokerPassword { get; set; }
    public string? SchemaRegistryUrl { get; set; }
    public string? SchemaRegistryUsername { get; set; }
    public string? SchemaRegistryPassword { get; set; }
    public AuthCredentialsSource SchemaRegistryBasicAuthCredentialsSource { get; set; } = AuthCredentialsSource.UserInfo;
    public int? SchemaRegistryMaxCachedSchemas { get; set; }
    public bool SchemaRegistryAutoRegisterSchemas { get; set; } = true;
    public Func<Type, string> TopicNamingConvention { get; set; } = type =>
        type.Name.ToLower(CultureInfo.InvariantCulture);

    public KafkaConfiguration(string bootstrapServers, string groupId)
        : this(bootstrapServers, groupId, groupId, AutoOffsetReset.Earliest)
    {
    }

    public KafkaConfiguration(string bootstrapServers, string groupId, AutoOffsetReset autoOffsetReset)
        : this(bootstrapServers, groupId, groupId, autoOffsetReset)
    {
    }

    public KafkaConfiguration(
        string bootstrapServers,
        string groupId,
        string clientId,
        AutoOffsetReset autoOffsetReset = AutoOffsetReset.Earliest,
        KafkaSerializationFormat serializationFormat = default,
        SecurityProtocol? brokerSecurityProtocol = null,
        SaslMechanism? brokerSaslMechanism = null,
        string? brokerUsername = null,
        string? brokerPassword = null,
        string? schemaRegistryUrl = null,
        string? schemaRegistryUsername = null,
        string? schemaRegistryPassword = null,
        AuthCredentialsSource schemaRegistryBasicAuthCredentialsSource = default,
        int? schemaRegistryMaxCachedSchemas = null,
        bool schemaRegistryAutoRegisterSchemas = false)
    {
        BootstrapServers = string.IsNullOrWhiteSpace(bootstrapServers)
            ? throw new ArgumentNullException(nameof(bootstrapServers))
            : bootstrapServers;
        GroupId = string.IsNullOrWhiteSpace(groupId)
            ? throw new ArgumentNullException(nameof(groupId))
            : groupId;
        ClientId = string.IsNullOrWhiteSpace(clientId) ? groupId : clientId;
        AutoOffsetReset = autoOffsetReset;
        SerializationFormat = serializationFormat;
        BrokerSecurityProtocol = brokerSecurityProtocol;
        BrokerSaslMechanism = brokerSaslMechanism;
        BrokerUsername = brokerUsername;
        BrokerPassword = brokerPassword;
        SchemaRegistryUrl = schemaRegistryUrl;
        SchemaRegistryUsername = schemaRegistryUsername;
        SchemaRegistryPassword = schemaRegistryPassword;
        SchemaRegistryBasicAuthCredentialsSource = schemaRegistryBasicAuthCredentialsSource;
        SchemaRegistryMaxCachedSchemas = schemaRegistryMaxCachedSchemas;
        SchemaRegistryAutoRegisterSchemas = schemaRegistryAutoRegisterSchemas;
    }

    public string GetTopicName(Type messageType)
    {
        ArgumentNullException.ThrowIfNull(messageType);
        return TopicNamingConvention(messageType);
    }

    internal void ApplyBrokerAuthentication(ClientConfig clientConfig)
    {
        ArgumentNullException.ThrowIfNull(clientConfig);

        if (string.IsNullOrWhiteSpace(BrokerUsername) || string.IsNullOrWhiteSpace(BrokerPassword))
            return;

        clientConfig.SecurityProtocol = BrokerSecurityProtocol ?? SecurityProtocol.SaslSsl;
        clientConfig.SaslMechanism = BrokerSaslMechanism ?? SaslMechanism.Plain;
        clientConfig.SaslUsername = BrokerUsername;
        clientConfig.SaslPassword = BrokerPassword;
    }

    internal SchemaRegistryConfig ToSchemaRegistryConfig()
    {
        if (string.IsNullOrWhiteSpace(SchemaRegistryUrl))
            throw new InvalidOperationException("SchemaRegistryUrl is required when using Json, Avro or Protobuf serialization.");

        var config = new SchemaRegistryConfig
        {
            Url = SchemaRegistryUrl
        };

        if (SchemaRegistryMaxCachedSchemas.HasValue)
            config.MaxCachedSchemas = SchemaRegistryMaxCachedSchemas.Value;

        if (!string.IsNullOrWhiteSpace(SchemaRegistryUsername) && !string.IsNullOrWhiteSpace(SchemaRegistryPassword))
        {
            config.BasicAuthCredentialsSource = SchemaRegistryBasicAuthCredentialsSource;
            config.BasicAuthUserInfo = $"{SchemaRegistryUsername}:{SchemaRegistryPassword}";
        }

        return config;
    }
}
