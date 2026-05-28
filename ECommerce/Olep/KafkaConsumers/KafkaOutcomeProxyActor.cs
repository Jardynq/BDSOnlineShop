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

            /*if (outcomeEvent.status != Status.OK)
            {
                // We currently have no reason to forward non OK events
                return;
            }*/

            // Forward to the relevant actors
            var actor = GrainFactory.GetGrain<ICustomerActor>(customerId);
            var analyticsActor = GrainFactory.GetGrain<IAnalyticsActor>(0);

            try
            {
                await Task.WhenAll(new Task[] {
                    actor.ProcessOutcome(outcomeEvent, null),
                    analyticsActor.UpdateAsync(outcomeEvent, null),
                });
            }
            catch (TimeoutException)
            {
                Console.WriteLine($"Outcome processing timed out for customer {customerId} in partition {this.id}");
                // TODO, should we retry or just fail the grain?
                throw;
            }
        }
    }
}
