using System.Reflection;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using Orleans.Concurrency;
using Orleans.Streams;
using Utilities;
using ECommerce.Kafka;

namespace ECommerce.Olep
{

    [Reentrant]
    public class CustomerActor : Grain, ICustomerActor
    {
        private long id;
        private double balance;

        private IStreamProvider streamProvider;

        private KafkaCheckoutConsumer consumer;

        public Task Init(double balance)
        {

            this.balance = balance;
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {   

            this.id = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(Constants.DefaultStreamProvider);
            var streamCheckoutIncoming = streamProvider.GetStream<Checkout>(Constants.CheckoutNamespace, this.id.ToString());
            await streamCheckoutIncoming.SubscribeAsync(ProcessCheckout);

            // Task 1, also subscribe to the outcome stream to update balance
            var streamOutcomeIncoming = streamProvider.GetStream<Outcome>(Constants.OutcomeNamespace, "0");
            await streamOutcomeIncoming.SubscribeAsync(DrawBalance);

            // Added consumer for task 2
            this.consumer = KafkaCheckoutConsumer.Build();
            Task.Run(() => this.consumer.SubscribeAndConsume(cancellationToken, streamIncoming));

        }

        // Task 1 implemented here
        private async Task ProcessCheckout(Checkout checkout, StreamSequenceToken token = null)
        {
            if (checkout.price * checkout.quantity > this.balance) {
                // Get outcome stream and send insufficient balance message to analytics actor            
                var outcomeStream = streamProvider.GetStream<Outcome>(Constants.OutcomeNamespace, "0");
                await outcomeStream.OnNextAsync(new Outcome(this.id, checkout.productId, checkout.price * checkout.quantity, Status.INSUFFICIENT_BALANCE), token);
                
                // Obs await might not be necessary since we return immediately after
                return;
            }

            // Get product stream and send inventory request to product actor
            var productStream = streamProvider.GetStream<Inventory>(Constants.InventoryNamespace, checkout.productId.ToString());
            await productStream.OnNextAsync(new Inventory(this.id, checkout.price, checkout.quantity), token);
            // Obs await might not be necessary since we return immediately after
            return;
        }

        // Task 1 implemented here
        private async Task DrawBalance(Outcome outcome, StreamSequenceToken token = null)
        {
            // Task 1, maybe not the right way to go about reducing the balance, since all the outcome messages are sent to the same stream
            if (outcome.customerId == this.id && outcome.status == Status.OK) {
                this.balance -= outcome.total;
            }
        }
    }
}

