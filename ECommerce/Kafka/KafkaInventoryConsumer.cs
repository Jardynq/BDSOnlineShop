// using Confluent.Kafka;
// using ECommerce.Olep.Schema;
// using Utilities;

// using Orleans.Concurrency;
// using Orleans.Streams;

// namespace ECommerce.Kafka;

// public class KafkaInventoryConsumer
// {
//     private readonly string topic;
//     private readonly IConsumer<Null, Inventory> consumer;

//     public static KafkaInventoryConsumer Build()
//     {
//         var config = new ConsumerConfig { 
//             BootstrapServers = Constants.kafkaService,
//             // Disable auto-committing of offsets.
//             EnableAutoCommit = false
//         };
//         var kafkaBuilder = new ConsumerBuilder<Null, Inventory>(config).SetValueDeserializer(new InventoryEventSerializer());
//         return new KafkaInventoryConsumer(kafkaBuilder.Build(), Constants.InventoryNamespace);
//     }


//     public KafkaInventoryConsumer(IConsumer<Null, Inventory> consumer, string topic)
//     {
//         this.consumer = consumer;
//         this.topic = topic;
//     }

//     public void SubscribeAndConsume(CancellationToken cancellationToken, IStreamProvider streamProvider)
//     {
//         this.consumer.Subscribe(topic);

//         // TODO implement the consume loop
//         while (!cancellationToken.IsCancellationRequested)
//         {
//             var consumeResult = consumer.Consume();
            
//             // handle consumed message.
//             // ...
            
//         }

//     }

// }

