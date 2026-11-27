# Muflone.Transport.Kafka

[![NuGet](https://img.shields.io/nuget/v/Muflone.Transport.Kafka.svg)](https://www.nuget.org/packages/Muflone.Transport.Kafka/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Muflone extension to manage messages on Kafka, designed for CQRS and Event Sourcing architectures.

## Breaking Changes

### v10.1.0 - Handler subscription alignment

Kafka now supports the same `IMessageSubscriber` and `MessageHandlersStarter` integration style used by `Muflone.Transport.RabbitMQ`.

Explicit consumers are still supported, but the transport can now also dispatch handlers through Muflone's subscriber infrastructure without relying on the old consumer-subscription approach.

---

## Features

- **CQRS Support** - Commands and events are transported through Kafka topics with consumer-group based routing
- **Kafka-native Messaging** - Producers publish serialized Muflone messages with `UserProperties` preserved as Kafka headers
- **Automatic Handler Dispatch** - Supports `IMessageSubscriber` and `MessageHandlersStarter`, aligned with the RabbitMQ transport
- **Explicit Consumers** - Supports `CommandConsumerBase`, `DomainEventsConsumerBase`, and `IntegrationEventsConsumerBase`
- **Topic Naming Convention** - Customize topic naming through `KafkaConfiguration.TopicNamingConvention`
- **Schema Registry Integration** - Supports Confluent Schema Registry for `Json`, `Avro`, and `Protobuf`
- **Basic Auth** - Supports SASL basic auth for Kafka brokers and HTTP Basic Auth for Schema Registry
- **Async-first** - All operations use async/await
- **.NET 10** - Built for `net10.0` with nullable reference types

## Install

```
dotnet add package Muflone.Transport.Kafka
```

or

```
Install-Package Muflone.Transport.Kafka
```

## Architecture Overview

Kafka does not differentiate between commands and events at the broker level, so the library models both with topics:

| Concept            | Transport   | Routing                                      | Consumer Behavior                       |
|--------------------|-------------|----------------------------------------------|-----------------------------------------|
| Commands           | Kafka topic | One logical consumer per consumer group      | Competing consumers                     |
| Domain Events      | Kafka topic | One-to-many across different consumer groups | Pub/sub through separate groups         |
| Integration Events | Kafka topic | One-to-many across different consumer groups | Pub/sub through separate groups         |

**Commands** are sent to a topic and processed by one consumer inside the same consumer group.
**Domain Events** and **Integration Events** are published to topics and can be consumed by multiple subscribers by using different consumer groups.

By default, each message type uses a topic named after the CLR type in lowercase:

- `CreateOrder` -> `createorder`
- `OrderCreated` -> `ordercreated`

## Quick Start

### 1. Define your messages

Commands and events must extend the Muflone base classes:

```csharp
public class CreateOrder : Command
{
    public readonly string OrderNumber;

    public CreateOrder(OrderId aggregateId, string orderNumber)
        : base(aggregateId)
    {
        OrderNumber = orderNumber;
    }
}

public class OrderCreated : DomainEvent
{
    public readonly string OrderNumber;

    public OrderCreated(OrderId aggregateId, string orderNumber)
        : base(aggregateId)
    {
        OrderNumber = orderNumber;
    }
}
```

### 2. Create command and event handlers

```csharp
public class CreateOrderCommandHandler : ICommandHandlerAsync<CreateOrder>
{
    private readonly IRepository _repository;

    public CreateOrderCommandHandler(IRepository repository)
    {
        _repository = repository;
    }

    public async Task HandleAsync(CreateOrder command, CancellationToken cancellationToken = default)
    {
        var order = Order.CreateOrder(command.AggregateId, command.OrderNumber);
        await _repository.SaveAsync(order, Guid.NewGuid(), cancellationToken);
    }
}

public class OrderCreatedEventHandler : IDomainEventHandlerAsync<OrderCreated>
{
    private readonly ILogger _logger;

    public OrderCreatedEventHandler(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger(GetType());
    }

    public Task HandleAsync(OrderCreated @event, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Order created: {OrderNumber}", @event.OrderNumber);
        return Task.CompletedTask;
    }
}
```

### 3. Create consumers

Consumers wire messages to their handlers. Extend the appropriate base class:

```csharp
// Command consumer (one handler per command)
public class CreateOrderConsumer : CommandConsumerBase<CreateOrder>
{
    protected override ICommandHandlerAsync<CreateOrder> HandlerAsync { get; }

    public CreateOrderConsumer(
        IRepository repository,
        KafkaConfiguration configuration,
        ILoggerFactory loggerFactory)
        : base(repository, configuration, loggerFactory)
    {
        HandlerAsync = new CreateOrderCommandHandler(repository);
    }
}

// Domain event consumer (supports multiple handlers per event)
public class OrderCreatedConsumer : DomainEventsConsumerBase<OrderCreated>
{
    protected override IEnumerable<IDomainEventHandlerAsync<OrderCreated>> HandlersAsync { get; }

    public OrderCreatedConsumer(
        KafkaConfiguration configuration,
        ILoggerFactory loggerFactory)
        : base(configuration, loggerFactory)
    {
        HandlersAsync = new List<IDomainEventHandlerAsync<OrderCreated>>
        {
            new OrderCreatedEventHandler(loggerFactory)
        };
    }
}

// Integration event consumer (for cross-boundary events)
public class OrderShippedConsumer : IntegrationEventsConsumerBase<OrderShipped>
{
    protected override IEnumerable<IIntegrationEventHandlerAsync<OrderShipped>> HandlersAsync { get; }

    public OrderShippedConsumer(
        KafkaConfiguration configuration,
        ILoggerFactory loggerFactory)
        : base(configuration, loggerFactory)
    {
        HandlersAsync = new List<IIntegrationEventHandlerAsync<OrderShipped>>
        {
            new OrderShippedIntegrationHandler(loggerFactory)
        };
    }
}
```

### 4. Register services in DI

```csharp
var builder = WebApplication.CreateBuilder(args);

var loggerFactory = LoggerFactory.Create(logging => logging.AddConsole());

// Configure Kafka connection
var kafkaConfiguration = new KafkaConfiguration(
    bootstrapServers: "localhost:9092",
    groupId: "OrderService",
    clientId: "OrderService",
    autoOffsetReset: AutoOffsetReset.Earliest
);

kafkaConfiguration.BrokerUsername = "kafka-user";
kafkaConfiguration.BrokerPassword = "kafka-password";
kafkaConfiguration.SchemaRegistryUrl = "https://localhost:8081";
kafkaConfiguration.SchemaRegistryUsername = "schema-user";
kafkaConfiguration.SchemaRegistryPassword = "schema-password";
kafkaConfiguration.SerializationFormat = KafkaSerializationFormat.Json;

// Register handlers
builder.Services.AddScoped<ICommandHandlerAsync<CreateOrder>, CreateOrderCommandHandler>();
builder.Services.AddScoped<IDomainEventHandlerAsync<OrderCreated>, OrderCreatedEventHandler>();

// Register Muflone Kafka transport
builder.Services.AddMufloneTransportKafka(loggerFactory, kafkaConfiguration);
```

If you want to register explicit consumers too:

```csharp
builder.Services.AddMufloneKafkaConsumers(
    new IConsumer[]
    {
        new CreateOrderConsumer(repository, kafkaConfiguration, loggerFactory),
        new OrderCreatedConsumer(kafkaConfiguration, loggerFactory)
    });
```

### 5. Send commands and publish events

```csharp
// Inject IServiceBus to send commands
public class OrderController : ControllerBase
{
    private readonly IServiceBus _serviceBus;

    public OrderController(IServiceBus serviceBus)
    {
        _serviceBus = serviceBus;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder(CreateOrderRequest request)
    {
        var command = new CreateOrder(
            new OrderId(Guid.NewGuid()),
            request.OrderNumber);

        await _serviceBus.SendAsync(command);
        return Accepted();
    }
}

// Inject IEventBus to publish events
public class OrderService
{
    private readonly IEventBus _eventBus;

    public OrderService(IEventBus eventBus)
    {
        _eventBus = eventBus;
    }

    public async Task NotifyOrderCreated(OrderId orderId, string orderNumber)
    {
        var @event = new OrderCreated(orderId, orderNumber);
        await _eventBus.PublishAsync(@event);
    }
}
```

## Configuration

`KafkaConfiguration` supports several constructor overloads:

```csharp
// Basic (default clientId = groupId, default AutoOffsetReset = Earliest)
new KafkaConfiguration(bootstrapServers, groupId);

// With explicit offset reset
new KafkaConfiguration(bootstrapServers, groupId, AutoOffsetReset.Earliest);

// With explicit clientId and offset reset
new KafkaConfiguration(bootstrapServers, groupId, clientId, AutoOffsetReset.Earliest);
```

| Parameter           | Description                                         | Default    |
|---------------------|-----------------------------------------------------|------------|
| `bootstrapServers`  | Kafka broker list, for example `localhost:9092`     | -          |
| `groupId`           | Consumer group id used by Kafka consumers           | -          |
| `clientId`          | Client id used by the producer and subscribers      | `groupId`  |
| `autoOffsetReset`   | Kafka offset reset strategy                         | `Earliest` |

### Broker basic auth

```csharp
kafkaConfiguration.BrokerUsername = "kafka-user";
kafkaConfiguration.BrokerPassword = "kafka-password";
kafkaConfiguration.BrokerSecurityProtocol = SecurityProtocol.SaslSsl;
kafkaConfiguration.BrokerSaslMechanism = SaslMechanism.Plain;
```

### Schema Registry and serialization format

The transport keeps using the configured Muflone `ISerializer` for the domain message payload and wraps that payload in a Kafka envelope managed through Schema Registry.

This means:

- existing Muflone messages do not need to become Avro or Protobuf classes
- Kafka producer/consumer wire format is controlled by `KafkaSerializationFormat`
- Schema Registry subjects are registered through the Confluent .NET serdes packages

```csharp
kafkaConfiguration.SchemaRegistryUrl = "https://localhost:8081";
kafkaConfiguration.SchemaRegistryUsername = "schema-user";
kafkaConfiguration.SchemaRegistryPassword = "schema-password";
kafkaConfiguration.SchemaRegistryAutoRegisterSchemas = true;
kafkaConfiguration.SerializationFormat = KafkaSerializationFormat.Avro;
// or KafkaSerializationFormat.Json
// or KafkaSerializationFormat.Protobuf
```


### Custom topic naming

```csharp
kafkaConfiguration.TopicNamingConvention = type => $"{type.Name.ToLowerInvariant()}";
```

## Registered Services

`AddMufloneTransportKafka` registers the following services as singletons:

| Interface            | Implementation         |
|----------------------|------------------------|
| `IServiceBus`        | `ServiceBus`           |
| `IEventBus`          | `ServiceBus`           |
| `IMessageSubscriber` | `KafkaSubscriber`      |
| `IHostedService`     | `KafkaBrokerStarter`   |
| `IHostedService`     | `MessageHandlersStarter` |

## Fully working example
TODO

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).
