using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Consumer.Models;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:29092",
    GroupId = "user-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

var schemaConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

const string topic = "users-avro-topic";

using var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

var consumerBuilder = new ConsumerBuilder<string, User>(config)
    .SetKeyDeserializer(Deserializers.Utf8)
    .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistry).AsSyncOverAsync());

using var consumer = consumerBuilder.Build();

consumer.Subscribe(topic);

Console.WriteLine("Esperando mensajes en el topic 'users-avro-topic'...");

try
{
    while (true)
    {
        var cr = consumer.Consume();
        var user = cr.Message.Value;

        Console.WriteLine($"👉 Usuario recibido: {user.Name}, edad: {user.Age}");
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Cancelado.");
}
finally
{
    consumer.Close();
}
