using Confluent.Kafka;
using ECommerce.Kafka;
using Orleans.Runtime;

namespace ECommerce.Olep.KafkaConsumers
{
    public abstract class KafkaProxyActorBase<TEvent> : Grain, IRemindable
        where TEvent : class
    {
        private const int BATCH_SIZE = 512;

        private bool isInitialized;
        private CancellationToken cancellationToken;
        private KafkaConsumer<TEvent> consumer;
        private IDisposable timer;

        protected int id;
        protected string topic;
        protected string group;

        protected virtual async Task Consume(ConsumeResult<long, TEvent> result) { }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.isInitialized = false;
            this.id = (int)this.GetPrimaryKeyLong();
            this.cancellationToken = cancellationToken;
            this.consumer = new KafkaConsumer<TEvent>(this.topic, this.group, id);
        }
        public override async Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
        {
            this.isInitialized = false;
            this.timer?.Dispose();
            this.consumer?.Dispose();
        }

        public Task StartFromBootstrap()
        {
            // Spread the heartbeats out over 10 seconds to avoid thundering herd on startup
            var startDelay = new Random().Next(60000, 70000);
            this.RegisterOrUpdateReminder("heartbeat", TimeSpan.FromMilliseconds(startDelay), TimeSpan.FromMinutes(1));
            InitAndStartPolling();
            return Task.CompletedTask;
        }

        public Task ReceiveReminder(string reminderName, TickStatus status)
        {
            if (reminderName == "heartbeat")
            {
                InitAndStartPolling();
            }
            return Task.CompletedTask;
        }

        public Task InitAndStartPolling()
        {
            if (isInitialized)
            {
                return Task.CompletedTask;
            }

            // Set up an Orleans timer to continuously poll Kafka
            Console.WriteLine($"Starting Kafka {this.topic} proxy for partition {this.id}...");
            DelayDeactivation(TimeSpan.MaxValue);
            timer = RegisterTimer(ConsumeAsync, null, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50));
            this.isInitialized = true;
            return Task.CompletedTask;
        }

        private async Task ConsumeAsync(object _)
        {
            // For some reason this gets cancelled even though the grain is active.
            // I have no idea why, or what to do about it.
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
