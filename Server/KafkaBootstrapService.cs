using ECommerce.Kafka;
using ECommerce.Olep.Interfaces;
using Microsoft.Extensions.Hosting;
using Utilities;

public class KafkaBootstrapService : BackgroundService
{
    private readonly IGrainFactory grainFactory;

    public KafkaBootstrapService(IGrainFactory grainFactory)
    {
        this.grainFactory = grainFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Maybe get this dynamically from Kafka metadata
        int numPartitions = 10;

        Task.WaitAll(new Task[]
            {
                KafkaTopic.InitTopic(Constants.CheckoutNamespace, numPartitions),
                KafkaTopic.InitTopic(Constants.InventoryNamespace, numPartitions),
                KafkaTopic.InitTopic(Constants.OutcomeNamespace, numPartitions),
            }
        );

        List<Task> tasks = new List<Task>();
        for (int i = 0; i < numPartitions; i++)
        {
            var checkoutConsumer = this.grainFactory.GetGrain<IKafkaCheckoutProxyActor>(i);
            tasks.Add(checkoutConsumer.StartFromBootstrap());

            var inventoryConsumer = this.grainFactory.GetGrain<IKafkaInventoryProxyActor>(i);
            tasks.Add(inventoryConsumer.StartFromBootstrap());

            var outcomeConsumer = this.grainFactory.GetGrain<IKafkaOutcomeProxyActor>(i);
            tasks.Add(outcomeConsumer.StartFromBootstrap());
        }
        await Task.WhenAll(tasks);
        Console.WriteLine("\n ***** All Kafka-proxies activated ***** \n");
    }
}