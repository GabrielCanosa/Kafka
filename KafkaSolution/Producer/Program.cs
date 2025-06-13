using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Producer.Models;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var kafkaConfig = builder.Configuration.GetSection("Kafka");
string bootstrapServers = kafkaConfig["BootstrapServers"]!;
string schemaRegistryUrl = kafkaConfig["SchemaRegistryUrl"]!;
string topic = kafkaConfig["Topic"]!;

// Endpoint para enviar usuario a Kafka
app.MapPost("/produce", async (User user) =>
{
    var producerConfig = new ProducerConfig
    {
        BootstrapServers = bootstrapServers
    };

    var schemaConfig = new SchemaRegistryConfig
    {
        Url = schemaRegistryUrl
    };

    using var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);
    var avroSerializer = new AvroSerializer<User>(schemaRegistry);

    using var producer = new ProducerBuilder<string, User>(producerConfig)
        .SetKeySerializer(Serializers.Utf8)
        .SetValueSerializer(avroSerializer)
        .Build();

    var message = new Message<string, User> { Key = user.Name, Value = user };

    var delivery = await producer.ProduceAsync(topic, message);
    return Results.Ok(new { delivery.TopicPartitionOffset });
});

app.Run();