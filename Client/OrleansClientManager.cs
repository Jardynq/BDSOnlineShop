using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Serialization;
using Utilities;

namespace Client
{
    public class OrleansClientManager
    {
        private IHost host;

        public OrleansClientManager()
        {
            this.host = new HostBuilder()
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
        }

        public async Task StopClient()
        {
            await this.host.StopAsync();
        }

        public async Task<IClusterClient> StartClient()
        {
            await this.host.StartAsync();
            return this.host.Services.GetService<IClusterClient>();
        }
    }
}