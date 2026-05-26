using Confluent.Kafka;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;

namespace ECommerce.Olep.KafkaConsumers
{
    public class KafkaOutcomeProxyActor : KafkaProxyActorBase<Outcome>, IKafkaOutcomeProxyActor
    {
        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.topic = Utilities.Constants.OutcomeNamespace;
            this.group = Utilities.Constants.OutcomeTopicGroup;
            await base.OnActivateAsync(cancellationToken);
        }

        protected override async Task Consume(ConsumeResult<long, Outcome> result)
        {
            var customerId = result.Message.Key;
            var outcomeEvent = result.Message.Value;

            // Forward to the relevant actor
            var actor = GrainFactory.GetGrain<ICustomerActor>(customerId);
            try
            {
                await actor.ProcessOutcome(outcomeEvent, null);
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"Customer outcome processing timed out for customer {customerId} in partition {this.id}");
                // TODO, should we retry or just fail the grain?
                throw;
            }

            // Forward to the analytics actor
            var analyticsActor = GrainFactory.GetGrain<IAnalyticsActor>(0);
            try
            {
                await analyticsActor.UpdateAsync(outcomeEvent, null);
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"Analytics outcome processing timed out for customer {customerId} in partition {this.id}");
                // TODO, should we retry or just fail the grain?
                throw;
            }
        }
    }
}
