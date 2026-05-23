using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using Orleans.Concurrency;
using Orleans.Streams;
using Utilities;

namespace ECommerce.Olep
{

    [Reentrant]
    public class CustomerActor : Grain, ICustomerActor
    {
        private long id;
        private double balance;

        private IStreamProvider streamProvider;

        public Task Init(double balance)
        {
            this.balance = balance;
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.id = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(Constants.DefaultStreamProvider);
            var streamIncoming = streamProvider.GetStream<Checkout>(Constants.CheckoutNamespace, this.id.ToString());
            await streamIncoming.SubscribeAsync(ProcessCheckout);

        }

        // Task 1 implemented here
        private async Task ProcessCheckout(Checkout checkout, StreamSequenceToken token = null)
        {
            if (checkout.price * checkout.quantity > this.balance) {
                // Get outcome stream and send insufficient balance message to analytics actor            
                var outcomeStream = streamProvider.GetStream<Outcome>(Constants.OutcomeNamespace, "0");
                outcomeStream.OnNextAsync(new Outcome(this.id, checkout.productId, checkout.price * checkout.quantity, Status.INSUFFICIENT_BALANCE), token);
                
                // No need to await the result of OnNextAsync since we return immediately after
                return;
            }

            // Get product stream and send inventory request to product actor
            var productStream = streamProvider.GetStream<Inventory>(Constants.InventoryNamespace, checkout.productId.ToString());
            productStream.OnNextAsync(new Inventory(this.id, checkout.price, checkout.quantity), token);

            // Again no need to await the result of OnNextAsync since we return immediately after
            return;
        }
    }
}

