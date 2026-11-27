// See https://aka.ms/new-console-template for more information

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Muflone;
using Muflone.Transport.Kafka;
using Muflone.Transport.Kafka.AppTests;
using Muflone.Transport.Kafka.Models;

HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

var kafkaConfiguration = new KafkaConfiguration("localhost:9092", "test-group", "test-group");
kafkaConfiguration.BrokerUsername = "kafka-user";
kafkaConfiguration.BrokerPassword = "kafka-password";
kafkaConfiguration.SchemaRegistryUrl = "https://localhost:8081";
kafkaConfiguration.SchemaRegistryUsername = "schema-user";
kafkaConfiguration.SchemaRegistryPassword = "schema-password";
kafkaConfiguration.SerializationFormat = KafkaSerializationFormat.Json;

builder.Services.AddMufloneTransportKafka(new NullLoggerFactory(), kafkaConfiguration);
builder.Services.AddHostedService<RunTest>();

builder.Services.AddDomainEventHandler<OrderCreatedEventHandler>();

using IHost host = builder.Build();

await host.RunAsync();
