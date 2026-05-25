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
        private IStreamProvider streamProvider;
        private int quantity;
        private double price;

        // static KafkaProducer producer = KafkaProducer.BuildCheckoutProducer();

        public Task Init(double price, int quantity)
        {
            this.price = price;
            this.quantity = quantity;
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.id = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(Constants.DefaultStreamProvider);
            var streamIncoming = streamProvider.GetStream<Inventory>(Constants.InventoryNamespace, this.id.ToString());
            await streamIncoming.SubscribeAsync(ProcessInventoryRequest);
        }

        // Task 1 implemented here
        private async Task ProcessInventoryRequest(Inventory inventory, StreamSequenceToken token)
        {
            // Check inventory quantity
            if (inventory.quantity > this.quantity) {
                // Add 90 units to the inventory if the current inventory is not enough
                this.quantity = inventory.quantity + 90;
            }
            this.quantity -= inventory.quantity;

            // Get outcome stream and send OK balance message to analytics actor            
            var outcomeStream = streamProvider.GetStream<Outcome>(Constants.OutcomeNamespace, "0");
            await outcomeStream.OnNextAsync(new Outcome(inventory.customerId, this.id, inventory.price * inventory.quantity, Status.OK), token);
            // Probably no need to await the result of OnNextAsync since we return immediately after
            
            return;
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

