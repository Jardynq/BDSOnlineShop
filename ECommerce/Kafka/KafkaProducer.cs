using Confluent.Kafka;
using Utilities;

namespace ECommerce.Kafka;

public class KafkaProducer<TEvent> : IDisposable
    where TEvent : class
{
    private readonly string topic;
    private readonly IProducer<long, TEvent> producer;

    public KafkaProducer(string topic)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = Constants.kafkaService,
            /*
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageSendMaxRetries = 10,
            RetryBackoffMs = 100,
            MessageTimeoutMs = 5000 // 5 seconds
            */
        };

        var kafkaBuilder = new ProducerBuilder<long, TEvent>(config)
                .SetValueSerializer(new EventSerializer<TEvent>());

        this.topic = topic;
        this.producer = kafkaBuilder.Build();
    }

    public async Task Append(long key, TEvent e)
    {
        // output the event to kafka (external service)
        // here we use the .NET kafka client implemented by Confluent
        await producer.ProduceAsync(topic, new Message<long, TEvent>
        {
            Key = key,
            Value = e
        });
    }

    // I don't think this method is firing, but it was created to ensure that the producer is properly closed when the proxy actor is disposed of
    // Might need to do something different
    public void Dispose()
    {
        producer.Flush(TimeSpan.FromSeconds(20)); // close after some time so (hopefully) all messages are sent
        producer.Dispose();
        Console.WriteLine("Producer closed.");
    }
}
