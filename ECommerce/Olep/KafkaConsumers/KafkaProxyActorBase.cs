using Confluent.Kafka;
using ECommerce.Kafka;

namespace ECommerce.Olep.KafkaConsumers
{
    public abstract class KafkaProxyActorBase<TEvent> : Grain
        where TEvent : class
    {
        private const int BATCH_SIZE = 512;

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
            // It seems we need to download an extra package.
            // So I tried the reminders, but for some reason the cancellation token is unrelated to the
            // grain deactivation, and just gets cancelled after like 10 seconds for some reason,
            // even if the grain is still active and the reminders cannot do anything.
            // So just ignore for now.
            /*
            if (this.cancellationToken.IsCancellationRequested)
            {
                this.timer?.Dispose();
                return;
            }
            */

            Dictionary<long, List<ConsumeResult<long, TEvent>>> batch = new();
            try
            {
                // Collect at most BATCH_SIZE by key
                // Poll once with a 0 timeout so we don't block the grain's single thread
                ConsumeResult<long, TEvent> last_event = null;
                for (int i = 0; i < BATCH_SIZE; i++)
                {
                    var result = consumer.Consume(TimeSpan.Zero);
                    if (result == null || result.Message == null)
                    {
                        break; // No new messages
                    }
                    last_event = result;
                    if (!batch.ContainsKey(result.Message.Key))
                    {
                        batch.Add(result.Message.Key, new());
                    }
                    batch[result.Message.Key].Add(result);
                }
                if (last_event == null)
                {
                    return; // No messages consumed
                }

                // Process events for separate keys in parallel,
                // but process events for the same key sequentially
                var tasks = batch.Select(async kvp =>
                {
                    foreach (var e in kvp.Value)
                    {
                        try
                        {
                            await Consume(e);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error processing event from {this.topic} for id {kvp.Key}: {ex.Message}");
                            throw;
                        }
                    }
                });

                try
                {
                    await Task.WhenAll(tasks);
                    consumer.Commit(last_event);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing message from {this.topic} partition {this.id}: {ex.Message}");
                    return;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in {topic} partition {this.id}: {ex.Message}");
                return;
            }
        }
    }
}
