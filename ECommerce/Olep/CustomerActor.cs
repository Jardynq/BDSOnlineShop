using ECommerce.Kafka;
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

        private KafkaProducer<Outcome> outcomeProducer;
        private KafkaProducer<Inventory> inventoryProducer;

        public Task Init(double balance)
        {
            this.balance = balance;
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.id = this.GetPrimaryKeyLong();
            this.outcomeProducer = new KafkaProducer<Outcome>(Constants.OutcomeNamespace);
            this.inventoryProducer = new KafkaProducer<Inventory>(Constants.InventoryNamespace);
        }

        public async Task ProcessCheckout(Checkout checkout, StreamSequenceToken token = null)
        {
            var total = checkout.price * checkout.quantity;
            if (total > this.balance)
            {
                // Get outcome log and send insufficient balance message to analytics actor            
                var outcome = new Outcome(this.id, checkout.productId, checkout.price * checkout.quantity, Status.INSUFFICIENT_BALANCE);
                //await outcomeProducer.Append(this.id, outcome);
                _ = outcomeProducer.Append(this.id, outcome);
                return;
            }

            // Reserve balance
            this.balance -= total;

            // Get product log and send inventory request to product actor
            var inventoryEvent = new Inventory(this.id, checkout.price, checkout.quantity);
            //await inventoryProducer.Append(checkout.productId, inventoryEvent);
            _ = inventoryProducer.Append(checkout.productId, inventoryEvent);
        }

        public async Task ProcessOutcome(Outcome outcome, StreamSequenceToken token = null)
        {
            // Realistically we should undo the reservation here in case the outcome failed,
            // however the outcome cannot fail after INSUFFICIENT_BALANCE has been checked,
            // (assuming at least once delivery), so just ignore for now.
            // Again, we cannot implement at least once as that requires deduplication,
            // but we cannot add an id to the event to distinguish them.

            // If, for example, the inventory could not resupply,
            // then we would need to undo the reservation here.
        }

        public Task<double> GetBalance()
        {
            return Task.FromResult(this.balance);
        }
    }
}
