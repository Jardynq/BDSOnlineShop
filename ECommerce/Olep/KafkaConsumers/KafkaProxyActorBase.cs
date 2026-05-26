using Confluent.Kafka;
using ECommerce.Kafka;

namespace ECommerce.Olep.KafkaConsumers
{
    public abstract class KafkaProxyActorBase<TEvent> : Grain
        where TEvent : class
    {
        protected int id;
        protected string topic;
        protected string group;
        protected CancellationToken cancellationToken;
        protected KafkaConsumer<TEvent> consumer;
        protected IDisposable timer;

        protected virtual async Task Consume(ConsumeResult<long, TEvent> result) { }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.id = (int)this.GetPrimaryKeyLong();
            this.cancellationToken = cancellationToken;
            this.consumer = new KafkaConsumer<TEvent>(this.topic, this.group, id);
        }

        public Task StartConsumingAsync()
        {
            // Set up an Orleans timer to continuously poll Kafka
            Console.WriteLine($"Starting Kafka {this.topic} proxy for partition {this.id}...");
            DelayDeactivation(TimeSpan.MaxValue);
            timer = RegisterTimer(ConsumeAsync, null, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50));
            return Task.CompletedTask;
        }

        private async Task ConsumeAsync(object state)
        {
            // For some reason this gets cancelled after like 10 seconds
            // So just ignore for now.
            // We could use reminders to consistenly heartbeat the grain
            // but for some reason I cannot find any reminder func in the orleans imports?
            // It seems we need to download an extra package which is not allowed.
            // So just ignore the cancellation token for now and pray.
            /*
            if (this.cancellationToken.IsCancellationRequested)
            {
                this.timer?.Dispose();
                return;
            }
            */

            try
            {
                // Poll once with a 0 timeout so we don't block the grain's single thread
                var result = consumer.Consume(TimeSpan.FromMilliseconds(10));
                if (result == null || result.Message == null)
                {
                    return; // No new messages
                }

                try
                {
                    await this.Consume(result);
                    consumer.Commit(result);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message from {this.topic} partition {this.id}: {ex.Message}");
                    return;
                }
            }
            catch (ConsumeException cEx)
            {
                Console.WriteLine($"Kafka failed to consume topic {topic}: {cEx.Error.Reason}");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in {topic} partition {this.id}: {ex.Message}");
                return;
            }
        }
    }
}
