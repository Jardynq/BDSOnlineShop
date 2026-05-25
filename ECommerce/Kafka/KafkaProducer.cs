using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ECommerce.Olep.Schema;
using Utilities;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using Orleans.Concurrency;
using Orleans.Streams;

namespace ECommerce.Kafka;
public class KafkaProducer
{

    private readonly string outputTopic;
    private readonly IProducer<Null, Checkout> producer;

    public static KafkaProducer BuildCheckoutProducer()
    {
        var config = new ProducerConfig {
            BootstrapServers = Constants.kafkaService
        };

        // Set up admin client to create checkout topic and set number of partions
        using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Constants.kafkaService }).Build())
        {
        try
            {
                adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification
                    {
                        Name              = Constants.CheckoutNamespace,
                        NumPartitions     = 1,
                        ReplicationFactor = 1
                    }
                }).GetAwaiter().GetResult();
            }
            catch (CreateTopicsException ex) {
                Console.WriteLine($"An error occured creating topic {Constants.CheckoutNamespace}: {ex.Results[0].Error.Reason}");
            }   
        }

        var kafkaBuilder = new ProducerBuilder<Null, Checkout>(config).SetValueSerializer(new CheckoutEventSerializer());

        return new KafkaProducer(kafkaBuilder.Build(), Constants.CheckoutNamespace);
    }

    private KafkaProducer(IProducer<Null, Checkout> producer, string outputTopic)
    {
        this.producer = producer;
        this.outputTopic = outputTopic;
    }

    public async Task Append(Checkout e)
    {
        // output the event to kafka (external service)
        // here we use the .NET kafka client implemented by Confluent
        await producer.ProduceAsync(outputTopic, new Message<Null, Checkout>
        {
            Timestamp = new Timestamp(Timestamp.UnixTimeEpoch, TimestampType.CreateTime),
            Value = e
        });
    }

}

