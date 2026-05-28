using ECommerce.Kafka;
using ECommerce.Olep.Checkpointing;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using MessagePack;
using Orleans.Concurrency;
using Orleans.Streams;
using Utilities;

namespace ECommerce.Olep
{
    [MessagePackObject]
    public class ProductActorState : ICloneable
    {
        [Key(0)]
        public int Quantity { get; set; }
        [Key(1)]
        public double Price { get; set; }

        public object Clone()
        {
            return new ProductActorState
            {
                Quantity = this.Quantity,
                Price = this.Price
            };
        }

        public ProductActorState()
        {
            this.Quantity = 0;
            this.Price = 0;
        }
    }

    [Reentrant]
    public class ProductActor : Grain, IProductActor
    {
        private long id;
        private ProductActorState state;

        private AsyncCheckpointer<ProductActorState> checkpointer;
        private IDisposable checkpointTimer;

        private KafkaProducer<Outcome> outcomeProducer;

        public Task Init(double price, int quantity)
        {
            this.state.Quantity = quantity;
            this.state.Price = price;
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.id = this.GetPrimaryKeyLong();
            this.outcomeProducer = new KafkaProducer<Outcome>(Constants.OutcomeNamespace);

            this.checkpointer = new AsyncCheckpointer<ProductActorState>("ProductActor", this.id, 1000, GetStateSnapshot);
            this.state = await this.checkpointer.LoadMostRecent();
            this.checkpointTimer = RegisterTimer(
                async _ => { checkpointer.Trigger(); },
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1)
             );
        }

        public ProductActorState GetStateSnapshot()
        {
            return (ProductActorState)this.state.Clone();
        }

        // Task 1 implemented here
        public async Task ProcessInventoryRequest(Inventory inventory, StreamSequenceToken token)
        {
            checkpointer.Tick();

            // Check inventory quantity
            if (this.state.Quantity < inventory.quantity)
            {
                // Add 90 units to the inventory if the current inventory is not enough
                // We do not remove inventory in this case by assignment description
                this.state.Quantity = inventory.quantity + 90;
            }
            else {
                this.state.Quantity -= inventory.quantity;
            }

            // Get outcome stream and send OK balance message to analytics actor            
            var outcomeEvent = new Outcome(inventory.customerId, this.id, inventory.price * inventory.quantity, Status.OK);
            //await outcomeProducer.Append(inventory.customerId, outcomeEvent);
            _ = outcomeProducer.Append(inventory.customerId, outcomeEvent);
        }

        public Task<double> GetPrice()
        {
            return Task.FromResult(this.state.Price);
        }

        public Task<int> GetInventory()
        {
            return Task.FromResult(this.state.Quantity);
        }
    }
}
