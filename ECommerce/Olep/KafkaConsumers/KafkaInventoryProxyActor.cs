using Confluent.Kafka;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using ECommerce.Olep.Token;

namespace ECommerce.Olep.KafkaConsumers
{
    public class KafkaInventoryProxyActor : KafkaProxyActorBase<Inventory>, IKafkaInventoryProxyActor
    {
        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.topic = Utilities.Constants.InventoryNamespace;
            this.group = Utilities.Constants.InventoryTopicGroup;
            await base.OnActivateAsync(cancellationToken);
        }

        protected override async Task Consume(ConsumeResult<long, Inventory> result)
        {
            var productId = result.Message.Key;
            var inventoryEvent = result.Message.Value;
            var timeStamp = result.Message.Timestamp.UtcDateTime.Ticks;

            // Forward to the relevant actor
            var actor = GrainFactory.GetGrain<IProductActor>(productId);
            try
            {
                var token = new ConcreteToken(timeStamp);
                await actor.ProcessInventoryRequest(inventoryEvent, token);
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"Inventory processing timed out for product {productId} in partition {this.id}");
                // TODO, should we retry or just fail the grain?
                throw;
            }
        }
    }
}
