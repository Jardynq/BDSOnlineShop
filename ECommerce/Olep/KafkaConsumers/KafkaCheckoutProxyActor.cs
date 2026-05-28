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
            var partition = result.Partition.Value;

            // Forward to the relevant actor
            var actor = GrainFactory.GetGrain<ICustomerActor>(customerId);
            try
            {
                var token = new KafkaSequenceToken(offset, partition);
                await actor.ProcessCheckout(checkoutEvent, token);
            }
            catch (TimeoutException)
            {
                // Throw an error so that the proxy actor base catches the error
                // The same offset is tried by this consumer the next time the consumer is called since it is not committed (by Consume base function since it is thrown)
                Console.WriteLine($"Checkout processing timed out for customer {customerId} in partition {this.id}");
                throw;
            }
        }
    }
}


