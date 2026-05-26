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
    public class CustomerActorState : ICloneable
    {
        [Key(0)]
        public double Balance { get; set; }

        public object Clone()
        {
            return new CustomerActorState
            {
                Balance = this.Balance
            };
        }

        public CustomerActorState()
        {
            this.Balance = 0;
        }
    }

    [Reentrant]
    public class CustomerActor : Grain, ICustomerActor
    {
        private long id;
        private CustomerActorState state;

        private AsyncCheckpointer<CustomerActorState> checkpointer;
        private IDisposable checkpointTimer;

        // Use this for kafka checkpointing. i.e. last kafka event that was gotten.
        // OR maybe not. Atleast we need to somehow checkpoint the current kafka state for this actor.
        // Instead of a guid, we can just use the timestamp as a hack.
        // If we recieve an event that has a lesser timestamp, then we know it's a duplicate and can ignore it.
        private int lastEventTimestamp;

        private KafkaProducer<Outcome> outcomeProducer;
        private KafkaProducer<Inventory> inventoryProducer;

        public Task Init(double balance)
        {
            this.state.Balance = balance;
            AsyncCheckpointer<CustomerActorState>.WriteStaticAsync(
                "CustomerActor",
                this.id,
                GetStateSnapshot()
            ).Wait();
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.id = this.GetPrimaryKeyLong();
            this.outcomeProducer = new KafkaProducer<Outcome>(Constants.OutcomeNamespace);
            this.inventoryProducer = new KafkaProducer<Inventory>(Constants.InventoryNamespace);

            this.checkpointer = new AsyncCheckpointer<CustomerActorState>("CustomerActor", this.id, 1000, GetStateSnapshot);
            this.state = await this.checkpointer.LoadMostRecent();
            this.checkpointTimer = RegisterTimer(
                async _ => { checkpointer.Trigger(); },
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1)
             );
        }


        public CustomerActorState GetStateSnapshot()
        {
            return (CustomerActorState)this.state.Clone();
        }

        public async Task ProcessCheckout(Checkout checkout, StreamSequenceToken token = null)
        {
            checkpointer.Tick();

            var total = checkout.price * checkout.quantity;
            if (total > this.state.Balance)
            {
                // Get outcome log and send insufficient balance message to analytics actor            
                var outcome = new Outcome(this.id, checkout.productId, checkout.price * checkout.quantity, Status.INSUFFICIENT_BALANCE);
                //await outcomeProducer.Append(this.id, outcome);
                _ = outcomeProducer.Append(this.id, outcome);
                return;
            }

            // Reserve balance
            this.state.Balance -= total;

            // Get product log and send inventory request to product actor
            var inventoryEvent = new Inventory(this.id, checkout.price, checkout.quantity);
            //await inventoryProducer.Append(checkout.productId, inventoryEvent);
            _ = inventoryProducer.Append(checkout.productId, inventoryEvent);
        }

        public async Task ProcessOutcome(Outcome outcome, StreamSequenceToken token = null)
        {
            checkpointer.Tick();

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
            return Task.FromResult(this.state.Balance);
        }
    }
}
