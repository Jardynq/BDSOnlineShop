using ECommerce.Kafka;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using Orleans.Concurrency;
using Orleans.Streams;
using Utilities;

namespace ECommerce.Olep
{
    [Reentrant]
    public class ProductActor : Grain, IProductActor
    {
        private long id;
        private int quantity;
        private double price;

        private KafkaProducer<Outcome> outcomeProducer;

        public Task Init(double price, int quantity)
        {
            this.price = price;
            this.quantity = quantity;
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.id = this.GetPrimaryKeyLong();
            this.outcomeProducer = new KafkaProducer<Outcome>(Constants.OutcomeNamespace);
        }

        // Task 1 implemented here
        public async Task ProcessInventoryRequest(Inventory inventory, StreamSequenceToken token)
        {
            // Check inventory quantity
            if (this.quantity < inventory.quantity)
            {
                // Add 90 units to the inventory if the current inventory is not enough
                this.quantity = inventory.quantity + 90;
            }
            this.quantity -= inventory.quantity;

            // Get outcome stream and send OK balance message to analytics actor            
            var outcomeEvent = new Outcome(inventory.customerId, this.id, inventory.price * inventory.quantity, Status.OK);
            await outcomeProducer.Append(inventory.customerId, outcomeEvent);
            // Probably no need to await the result of OnNextAsync since we return immediately after
        }

        public Task<double> GetPrice()
        {
            return Task.FromResult(this.price);
        }

        public Task<int> GetInventory()
        {
            return Task.FromResult(this.quantity);
        }
    }
}
