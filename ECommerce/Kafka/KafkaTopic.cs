using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Utilities;

namespace ECommerce.Kafka
{
    public static class KafkaTopic
    {
        public static async Task InitTopic(string topic, int partitions)
        {
            using var admin = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = Constants.kafkaService }
            ).Build();

            try
            {
                Console.WriteLine($"Attempting to create topic {topic}");
                await admin.CreateTopicsAsync(new[]
                {
                new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = partitions,
                    ReplicationFactor = 1
                }
            });
            }
            catch (CreateTopicsException e)
            {
                // Topic might already exist, which is fine.
                if (e.Results.Any(r => r.Error.Code != Confluent.Kafka.ErrorCode.TopicAlreadyExists))
                {
                    Console.WriteLine($"An error occurred creating topic {topic}");
                    throw;
                }
                else
                {
                    Console.WriteLine($"Topic {topic} already exists");
                }
            }
        }
    }
}
