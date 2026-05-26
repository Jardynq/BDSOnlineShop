using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ECommerce.Olep.Schema;
using Utilities;

namespace ECommerce.Kafka;

public class KafkaProducer : IDisposable
{
    private static readonly int numberOfPartitions = 10;

    private readonly string outputTopic;
    private readonly IProducer<long, Checkout> producer;

    public static KafkaProducer BuildCheckoutProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = Constants.kafkaService
        };

        // Set up admin client to create checkout topic and set number of partions
        using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Constants.kafkaService }).Build())
        {

            // This is done each time a new thread is created, which for an experiment with 8 threads throws a lot of errors if not caught
            // We should change this logic
            // Currently I must close the docker container to restart Kafka entirely
            try
            {
                adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification
                    {
                        Name = Constants.CheckoutNamespace,
                        NumPartitions = numberOfPartitions,
                        ReplicationFactor = 1
                    }
                }).GetAwaiter().GetResult();
                Console.WriteLine($"\n ** Setup ** : Kafka topic {Constants.CheckoutNamespace} created with {numberOfPartitions} partitions.");
            }
            catch (CreateTopicsException ex)
            {
                // Console.WriteLine($"An error occured creating topic {Constants.CheckoutNamespace}: {ex.Results[0].Error.Reason}");
            }
        }

        var kafkaBuilder = new ProducerBuilder<long, Checkout>(config).SetValueSerializer(new CheckoutEventSerializer());
        // Console.WriteLine($"Producer connecting to {Constants.kafkaService}, topic {Constants.CheckoutNamespace}");
        return new KafkaProducer(kafkaBuilder.Build(), Constants.CheckoutNamespace);
    }

    private KafkaProducer(IProducer<long, Checkout> producer, string outputTopic)
    {
        this.producer = producer;
        this.outputTopic = outputTopic;
    }

    public async Task Append(Checkout e)
    {
        // output the event to kafka (external service)
        // here we use the .NET kafka client implemented by Confluent
        await producer.ProduceAsync(outputTopic, new Message<long, Checkout>
        {
            Timestamp = new Timestamp(Timestamp.UnixTimeEpoch, TimestampType.CreateTime),
            Key = e.customerId,
            Value = e
        });
    }

    // I don't think this method is firing, but it was created to ensure that the consumer is properly closed when the proxy actor is disposed of
    // Might need to do something different
    public void Dispose()
    {
        producer.Flush(TimeSpan.FromSeconds(20)); // close after some time so (hopefully) all messages are sent
        producer.Dispose();
        Console.WriteLine("Producer closed.");
    }
}
