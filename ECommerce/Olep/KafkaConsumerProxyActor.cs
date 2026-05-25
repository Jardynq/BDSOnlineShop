using System.Reflection;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using Orleans.Concurrency;
using Orleans.Streams;
using Utilities;
using ECommerce.Kafka;

// Added consumer for task 2
namespace ECommerce.Olep
{

    [Reentrant]
    public class KafkaConsumerProxyActor : Grain, IProxyActor
    {

        private IStreamProvider streamProvider;

        private KafkaCheckoutConsumer consumer;

        CancellationToken cancellationToken;

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {   
            this.cancellationToken = cancellationToken;
            this.streamProvider = this.GetStreamProvider(Constants.DefaultStreamProvider);
        }
        public Task Init()
        {
            // Console.WriteLine("Initializing KafkaConsumerProxyActor and starting Kafka consumer...");
            this.consumer = KafkaCheckoutConsumer.Build();
            // Task.Run(() => ... );
            _ = Task.Run(() => this.consumer.SubscribeAndConsume(cancellationToken, this.streamProvider))
                .ContinueWith(t => Console.WriteLine($"Consumer crashed: {t.Exception}"), 
                            TaskContinuationOptions.OnlyOnFaulted);
            return Task.CompletedTask;
        }

    }
}

