using Confluent.Kafka;
using ECommerce.Olep.Schema;
using Utilities;

using Orleans.Concurrency;
using Orleans.Streams;

namespace ECommerce.Kafka;

public class KafkaCheckoutConsumer
{
    private readonly string topic;
    private readonly IConsumer<Null, Checkout> consumer;

    public static KafkaCheckoutConsumer Build()
    {
        var config = new ConsumerConfig { 
            BootstrapServers = Constants.kafkaService,
            // Disable auto-committing of offsets.
            EnableAutoCommit = false,
            GroupId = "checkout-consumer-group", // Added group id for consumer group management among checkout topic
            AutoOffsetReset = AutoOffsetReset.Earliest // Added auto offset reset to earliest to ensure we consume all messages from the beginning if no committed offsets are found
        };
        var kafkaBuilder = new ConsumerBuilder<Null, Checkout>(config).SetValueDeserializer(new CheckoutEventSerializer());
        return new KafkaCheckoutConsumer(kafkaBuilder.Build(), Constants.CheckoutNamespace);
    }

    public KafkaCheckoutConsumer(IConsumer<Null, Checkout> consumer, string topic)
    {
        this.consumer = consumer;
        this.topic = topic;
    }

    // Implemented for task 2
    public void SubscribeAndConsume(CancellationToken cancellationToken, IAsyncStream<Checkout> stream)
    {
        this.consumer.Subscribe(topic);

        // Consume loop
        while (!cancellationToken.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume();
        
            // Extra check to ensure that the topic is the checkout topic
            // This should always be the case due to to the static builder method of this class
            if (topic == Constants.CheckoutNamespace)
            {
                stream.OnNextAsync(consumeResult.Message.Value);
                // ^ Obs. cannot wait since it isn't asyncronous
                // Most likely we can return immediately after since the message is sent to the stream

                return;
            }
            // Throw error here

            
        }

    }

}

