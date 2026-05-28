using ECommerce.Olep.Checkpointing;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using MessagePack;
using Orleans.Concurrency;
using Orleans.Streams;
using System.Text.Json;

namespace ECommerce.Olep
{
    [MessagePackObject]
    public class AnalyticsActorState : ICloneable
    {
        [Key(0)]
        public Dictionary<long, double> Query { get; set; }
        [Key(1)]
        public Dictionary<long, int> DebugQuery { get; set; }

        public object Clone()
        {
            // Why is C# like this...
            string temp = JsonSerializer.Serialize(this);
            return JsonSerializer.Deserialize<AnalyticsActorState>(temp);
        }

        public AnalyticsActorState()
        {
            this.Query = new Dictionary<long, double>();
            this.DebugQuery = new Dictionary<long, int>();
        }
    }

    [Reentrant]
    public class AnalyticsActor : Grain, IAnalyticsActor
    {
        private AnalyticsActorState state;

        private AsyncCheckpointer<AnalyticsActorState> checkpointer;
        private IDisposable checkpointTimer;

        public Task Init()
        {
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.checkpointer = new AsyncCheckpointer<AnalyticsActorState>("AnalyticsActor", 0, 10000, GetStateSnapshot);
            this.state = await this.checkpointer.LoadMostRecent();
            this.checkpointTimer = RegisterTimer(
                async _ => { checkpointer.Trigger(); },
                null,
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(1)
             );
        }

        public AnalyticsActorState GetStateSnapshot()
        {
            return (AnalyticsActorState)this.state.Clone();
        }

        public Task UpdateAsync(Outcome outcome, StreamSequenceToken token = null)
        {
            checkpointer.Tick();

            // If checkout is successful, update the total sales for the corresponding product
            if (outcome.status == Status.OK)
            {
                var previous = this.state.Query.GetValueOrDefault(outcome.customerId, 0);
                this.state.Query[outcome.customerId] = previous + outcome.total;
            }
            // Increment the debug counter for end-to-end latency metrics.
            var previousCount = this.state.DebugQuery.GetValueOrDefault(outcome.customerId, 0);
            this.state.DebugQuery[outcome.customerId] = previousCount + 1;
            return Task.CompletedTask;
        }

        public async Task<List<KeyValuePair<long, double>>> Top10()
        {
            // Get top ten customers by their value
            var top10 = this.state.Query.OrderByDescending(kv => kv.Value).Take(10).ToList();
            return await Task.FromResult(top10);
        }

        public async Task<int> CustomerOutcomeProcessedCount(long customerId)
        {
            return this.state.DebugQuery.GetValueOrDefault(customerId, 0);
        }
    }
}
