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
            var offset = result.Offset.Value;
            var partition = result.Partition.Value;

            // Forward to the relevant actor
            var actor = GrainFactory.GetGrain<IProductActor>(productId);
            try
            {
                var token = new KafkaSequenceToken(offset, partition);
                await actor.ProcessInventoryRequest(inventoryEvent, token);
            }
            catch (TimeoutException)
            {
                // Throw an error so that the proxy actor base catches the error
                // The same offset is tried by this consumer the next time the consumer is called since it is not committed (by Consume base function since it is thrown)
                Console.WriteLine($"Inventory processing timed out for product {productId} in partition {this.id}");
                throw;
            }
        }
    }
}
