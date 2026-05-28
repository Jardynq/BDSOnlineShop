using ECommerce.Kafka;
using ECommerce.Olep.Checkpointing;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using ECommerce.Olep.Token;
using MessagePack;
using Orleans.Concurrency;
using Orleans.Streams;
using System.Text.Json;
using Utilities;

namespace ECommerce.Olep
{
    [MessagePackObject]
    public class CustomerActorState : ICloneable
    {
        [Key(0)]
        public double Balance { get; set; }
        // Event number to be able to ignore duplicates 
        [Key(1)]
        public Dictionary<int, long> LastCheckoutEventSequenceNumbers { get; set; }
        [Key(2)]
        public Dictionary<int, long> LastOutcomeEventSequenceNumbers { get; set; }
        public object Clone()
        {
            string temp = JsonSerializer.Serialize(this);
            return JsonSerializer.Deserialize<CustomerActorState>(temp);
        }

        public CustomerActorState()
        {
            this.Balance = 0;
            this.LastCheckoutEventSequenceNumbers = new Dictionary<int, long>();
            this.LastOutcomeEventSequenceNumbers = new Dictionary<int, long>();
        }
    }

    [Reentrant]
    public class CustomerActor : Grain, ICustomerActor
    {
        private long id;
        private CustomerActorState state;

        private AsyncCheckpointer<CustomerActorState> checkpointer;
        private IDisposable checkpointTimer;

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

            if (token is KafkaSequenceToken concreteToken)
            {
                // Check if the event is a duplicate by comparing the sequence number (timestamp) with the last processed event
                if (this.state.LastCheckoutEventSequenceNumbers.TryGetValue(concreteToken.EventIndex, out long lastSequenceNumber))
                {
                    if (concreteToken.SequenceNumber <= lastSequenceNumber)
                    {
                        // If so, just return
                        Console.WriteLine($"Duplicate event detected for customer actor {this.id} in checkout processing");
                        return;
                    }
                }
            }

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

            // The request has now been processed fully and we note the sequence number (timestamp) on the token to be able to ignore duplicates later
            if (token is KafkaSequenceToken _concreteToken)
            {
                if (state.LastCheckoutEventSequenceNumbers.ContainsKey(_concreteToken.EventIndex))
                {
                    state.LastCheckoutEventSequenceNumbers[_concreteToken.EventIndex] = _concreteToken.SequenceNumber;
                }
                else
                {
                    state.LastCheckoutEventSequenceNumbers.Add(_concreteToken.EventIndex, _concreteToken.SequenceNumber);
                }
            }
        }

        public async Task ProcessOutcome(Outcome outcome, StreamSequenceToken token = null)
        {
            if (token is KafkaSequenceToken concreteToken)
            {
                // Check if the event is a duplicate by comparing the sequence number (timestamp) with the last processed event
                if (this.state.LastOutcomeEventSequenceNumbers.TryGetValue(concreteToken.EventIndex, out long lastSequenceNumber))
                {
                    if (concreteToken.SequenceNumber <= lastSequenceNumber)
                    {
                        // If so, just write to console and return 
                        // Console.WriteLine($"Duplicate event detected for customer actor {this.id} in outcome processing");
                        return;
                    }
                }
            }

            // Realistically we should undo the reservation here in case the outcome failed,
            // however the outcome cannot fail after INSUFFICIENT_BALANCE has been checked,
            // (assuming at least once delivery), so just ignore for now.
            // Again, we cannot implement at least once as that requires deduplication,
            // but we cannot add an id to the event to distinguish them.

            // If, for example, the inventory could not resupply,
            // then we would need to undo the reservation here.

            // The request has now been processed fully and we note the sequence number (timestamp) on the token to be able to ignore duplicates later
            if (token is KafkaSequenceToken _concreteToken)
            {
                if (state.LastOutcomeEventSequenceNumbers.ContainsKey(_concreteToken.EventIndex))
                {
                    state.LastOutcomeEventSequenceNumbers[_concreteToken.EventIndex] = _concreteToken.SequenceNumber;
                }
                else
                {
                    state.LastOutcomeEventSequenceNumbers.Add(_concreteToken.EventIndex, _concreteToken.SequenceNumber);
                }
            }
        }

        public Task<double> GetBalance()
        {
            return Task.FromResult(this.state.Balance);
        }
    }
}
