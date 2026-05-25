using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Serialization;
using Utilities;

namespace Client
{
    public static class OrleansClientManager
    {
        public static async Task<IClusterClient> GetClient()
        {
            var client = new HostBuilder()
                .UseOrleansClient(clientBuilder =>
                {
                    clientBuilder.UseLocalhostClustering();
                    clientBuilder.Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = Constants.ClusterId;
                        options.ServiceId = Constants.ServiceId;
                    });
                    clientBuilder.AddMemoryStreams(Constants.DefaultStreamProvider);
                })
                .ConfigureLogging(loggingBuilder => loggingBuilder.AddConsole())
                .ConfigureServices(f => f.AddSerializer(ser =>
                {
                    ser.AddNewtonsoftJsonSerializer(isSupported: type => type.Namespace.StartsWith("ECommerce.Olep"));
                }))
                .Build();

            await client.StartAsync();

            return client.Services.GetService<IClusterClient>();
        }
    }
}