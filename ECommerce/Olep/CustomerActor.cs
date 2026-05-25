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
    public class CustomerActor : Grain, ICustomerActor
    {
        private long id;
        private double balance;

        private IStreamProvider streamProvider;
        private StreamId checkoutStreamId;
        private StreamId outcomeStreamId;

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
            this.checkoutStreamId = StreamId.Create(Constants.CheckoutNamespace, this.id.ToString());
            this.outcomeStreamId = StreamId.Create(Constants.OutcomeNamespace, "0");

            // Task 1 subscriptions
            var checkoutStream = streamProvider.GetStream<Checkout>(this.checkoutStreamId);
            await checkoutStream.SubscribeAsync(ProcessCheckout);

            var outcomeStream = streamProvider.GetStream<Outcome>(this.outcomeStreamId);
            await outcomeStream.SubscribeAsync(ProcessOutcome);

            // Added consumer for task 2
            this.consumer = KafkaCheckoutConsumer.Build();
            _ = Task.Run(() => this.consumer.SubscribeAndConsume(cancellationToken, checkoutStream));
            // Why are we not awaiting this???
        }

        // Task 1 implemented here
        private async Task ProcessCheckout(Checkout checkout, StreamSequenceToken token = null)
        {
            var total = checkout.price * checkout.quantity;
            if (total > this.balance)
            {
                // Get outcome stream and send insufficient balance message to analytics actor            
                var outcomeStream = streamProvider.GetStream<Outcome>(outcomeStreamId);
                var outcome = new Outcome(this.id, checkout.productId, checkout.price * checkout.quantity, Status.INSUFFICIENT_BALANCE);
                await outcomeStream.OnNextAsync(outcome, token);
                return;
            }

            // Get product stream and send inventory request to product actor
            var inventoryStreamId = StreamId.Create(Constants.InventoryNamespace, checkout.productId.ToString());
            var inventoryStream = streamProvider.GetStream<Inventory>(inventoryStreamId);
            var inventoryEvent = new Inventory(this.id, checkout.price, checkout.quantity);

            try
            {
                // Reserve balance
                this.balance -= total;
                await inventoryStream.OnNextAsync(inventoryEvent, token);
            }
            catch (Exception)
            {
                // We sadly cannot know if the event was handled in case of timeout,
                // so we cannot undo here. Instead we must wait for the outcome event.
                // But even that event can possibly not be delivered.
                // Fixing this requires changes we are not allowed to do,
                // so simply allow this inconsistency and discuss in the report.
                throw;
            }
        }

        private async Task ProcessOutcome(Outcome outcome, StreamSequenceToken token = null)
        {
            // Realistically we should undo the reservation here in case the outcome failed,
            // however the outcome cannot fail after INSUFFICIENT_BALANCE has been checked,
            // (assuming at least once delivery), so just ignore for now.
            // Again, we cannot implement at least once as that requires deduplication,
            // but we cannot add an id to the event to distinguish them.
        }

        public Task<double> GetBalance()
        {
            return Task.FromResult(this.balance);
        }
    }
}
