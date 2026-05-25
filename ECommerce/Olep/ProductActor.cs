using ECommerce.Kafka;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using Orleans.Concurrency;
using Orleans.Runtime;
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

        private IStreamProvider streamProvider;
        private StreamId inventoryStreamId;
        private StreamId outcomeStreamId;

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
            this.outcomeStreamId = StreamId.Create(Constants.OutcomeNamespace, "0");
            this.inventoryStreamId = StreamId.Create(Constants.InventoryNamespace, this.id.ToString());

            var inventoryStream = streamProvider.GetStream<Inventory>(this.inventoryStreamId);
            await inventoryStream.SubscribeAsync(ProcessInventoryRequest);
        }

        // Task 1 implemented here
        private async Task ProcessInventoryRequest(Inventory inventory, StreamSequenceToken token)
        {


            // Check inventory quantity
            if (this.quantity < inventory.quantity)
            {
                // Add 90 units to the inventory if the current inventory is not enough
                this.quantity = inventory.quantity + 90;
            }
            this.quantity -= inventory.quantity;

            // Get outcome stream and send OK balance message to analytics actor            
            var outcomeStream = streamProvider.GetStream<Outcome>(this.outcomeStreamId);
            var outcomeEvent = new Outcome(inventory.customerId, this.id, inventory.price * inventory.quantity, Status.OK);
            await outcomeStream.OnNextAsync(outcomeEvent, token);
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
