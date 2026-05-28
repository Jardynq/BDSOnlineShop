using Confluent.Kafka;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using Orleans.Concurrency;
using ECommerce.Olep.Token;
namespace ECommerce.Olep.KafkaConsumers
{


    [Reentrant]
    public class KafkaCheckoutProxyActor : KafkaProxyActorBase<Checkout>, IKafkaCheckoutProxyActor
    {
        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.topic = Utilities.Constants.CheckoutNamespace;
            this.group = Utilities.Constants.CheckoutTopicGroup;
            await base.OnActivateAsync(cancellationToken);
        }

        protected override async Task Consume(ConsumeResult<long, Checkout> result)
        {
            var customerId = result.Message.Key;
            var checkoutEvent = result.Message.Value;
            var offset = result.Offset.Value;

            // Forward to the relevant actor
            var actor = GrainFactory.GetGrain<ICustomerActor>(customerId);
            try
            {
                var token = new ConcreteToken(offset, this.id);
                await actor.ProcessCheckout(checkoutEvent, token);
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"Checkout processing timed out for customer {customerId} in partition {this.id}");
                // TODO, should we retry or just fail the grain?
                throw;
            }
        }
    }
}


