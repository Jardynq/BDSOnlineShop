using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Serialization;
using Utilities;

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
            .AddMemoryStreams(Constants.DefaultStreamProvider)
            .AddMemoryGrainStorage("PubSubStore")
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
    .ConfigureServices(services =>
    {
        // Ensure that a Kafka consumer proxy actor is created for each partition of each topic.
        services.AddHostedService<KafkaBootstrapService>();
    })
    .Build();

await host.StartAsync();
Console.WriteLine("\n *************************************************************************");
Console.WriteLine("    The BDS OnlineShop server is started. Press Enter to terminate...    ");
Console.WriteLine("\n *************************************************************************");
Console.ReadLine();
await host.StopAsync();
