using Confluent.Kafka;
using ECommerce.Olep.Schema;
using Utilities;

using Orleans.Streams;
using Orleans.Concurrency;
using Orleans.Streams;

namespace ECommerce.Kafka;

public class KafkaCheckoutConsumer : IDisposable
{
    private readonly string topic;
    private readonly IConsumer<long, Checkout> consumer;

    public static KafkaCheckoutConsumer Build()
    {
        var config = new ConsumerConfig { 
            BootstrapServers = Constants.kafkaService,
            // Disable auto-committing of offsets.
            EnableAutoCommit = false,
            GroupId = "checkout-consumer-group", // Added group id for consumer group management among checkout topic
            AutoOffsetReset = AutoOffsetReset.Earliest // Added auto offset reset to earliest to ensure we consume all messages from the beginning if no committed offsets are found
        };
        var kafkaBuilder = new ConsumerBuilder<long, Checkout>(config).SetValueDeserializer(new CheckoutEventSerializer());
        return new KafkaCheckoutConsumer(kafkaBuilder.Build(), Constants.CheckoutNamespace);
    }

    public KafkaCheckoutConsumer(IConsumer<long, Checkout> consumer, string topic)
    {
        this.consumer = consumer;
        this.topic = topic;
    }

    // Implemented for task 2
    public async Task SubscribeAndConsume(CancellationToken cancellationToken, IStreamProvider streamProvider)
    {
        this.consumer.Subscribe(topic);

        // Consume loop
        // Does not timeout, which it probably should
        while (!cancellationToken.IsCancellationRequested)
        {

            var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

            if (consumeResult == null)
            {   
                continue;
            }
            IAsyncStream<Checkout> customerStream = streamProvider.GetStream<Checkout>( Constants.CustomerNamespace, consumeResult.Message.Value.customerId.ToString() );
            await customerStream.OnNextAsync(consumeResult.Message.Value); 
            consumer.Commit(consumeResult);
        }
    }

    // I don't think this method is firing, but it was created to ensure that the consumer is properly closed when the proxy actor is disposed of
    // Might need to do something different
    public void Dispose()
    {
        consumer.Close();
        consumer.Dispose();
        Console.WriteLine("Consumer closed.");
    }
}

