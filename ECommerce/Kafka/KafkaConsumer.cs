using Confluent.Kafka;
using Utilities;

namespace ECommerce.Kafka;

public class KafkaConsumer<TEvent> : IDisposable
    where TEvent : class
{
    private readonly string topic;
    private readonly IConsumer<long, TEvent> consumer;

    public KafkaConsumer(string topic, string group, int partition)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = Constants.kafkaService,
            EnableAutoCommit = false, // Disable auto-committing of offsets.
            GroupId = group, // Added group id for consumer group management among checkout topic
            AutoOffsetReset = AutoOffsetReset.Earliest // Added auto offset reset to earliest to ensure we consume all messages from the beginning if no committed offsets are found
        };
        var kafkaBuilder = new ConsumerBuilder<long, TEvent>(config)
            .SetValueDeserializer(new EventSerializer<TEvent>());

        this.topic = topic;
        this.consumer = kafkaBuilder.Build();
        this.consumer.Subscribe(topic);
    }

    // Implemented for task 2
    // This function can be optimized since each customer might have multiple messages in the topic,
    // and we can batch process them instead of each message individually.
    // I.e. we only need to ensure that each customer has their events in order,
    // but we can process multiple customers in parallel.
    public ConsumeResult<long, TEvent> Consume(CancellationToken cancellationToken)
    {
        return consumer.Consume(cancellationToken);
    }
    public ConsumeResult<long, TEvent> Consume(TimeSpan timeout)
    {
        return consumer.Consume(timeout);
    }

    public void Commit(ConsumeResult<long, TEvent> result)
    {
        consumer.Commit(result);
    }

    // Method created to ensure that the consumer is properly closed when the proxy actor is disposed of
    // Might need to do something different
    public void Dispose()
    {
        consumer.Close();
        consumer.Dispose();
        Console.WriteLine("Consumer closed.");
    }
}
