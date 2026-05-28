using ECommerce.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Serialization;
using Utilities;

const int NUM_PARTITIONS = 10;

using var host = new HostBuilder()
    .UseOrleans(builder =>
    {
        builder
            .UseLocalhostClustering()
            .Configure<ClusterOptions>(options =>
            {
                options.ClusterId = Constants.ClusterId;
                options.ServiceId = Constants.ServiceId;
            })
            .UseInMemoryReminderService()
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Warning);
            })
            .Services.AddSerializer(ser =>
            {
                ser.AddNewtonsoftJsonSerializer(isSupported: type => type.Namespace.StartsWith("ECommerce.Olep"));
            });
    })
    .Build();

Task.WaitAll(new Task[]
    {   KafkaTopic.InitTopic(Constants.CheckoutNamespace, NUM_PARTITIONS),
        KafkaTopic.InitTopic(Constants.InventoryNamespace, NUM_PARTITIONS),
        KafkaTopic.InitTopic(Constants.OutcomeNamespace, NUM_PARTITIONS),
    }
);

await host.StartAsync();
Console.WriteLine("\n *************************************************************************");
Console.WriteLine("    The BDS OnlineShop server is started. Press Enter to terminate...    ");
Console.WriteLine("\n *************************************************************************");
Console.ReadLine();
await host.StopAsync();
