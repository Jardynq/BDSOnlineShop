using ECommerce.Olep.Checkpointing;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using MessagePack;
using Orleans.Concurrency;
using Orleans.Streams;
using System.Text.Json;
using ECommerce.Olep.Token;

namespace ECommerce.Olep
{
    [MessagePackObject]
    public class AnalyticsActorState : ICloneable
    {
        [Key(0)]
        public Dictionary<long, double> Query { get; set; }
        [Key(1)]
        public long LastOutcomeEventSequenceNumber { get; set; }
        public object Clone()
        {
            // Why is C# like this...
            string temp = JsonSerializer.Serialize(this);
            return JsonSerializer.Deserialize<AnalyticsActorState>(temp);
        }

        public AnalyticsActorState()
        {
            this.Query = new Dictionary<long, double>();
            this.LastOutcomeEventSequenceNumber = 0;
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
            // Clear the query dict between workload runs
            this.state.Query.Clear();
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
            if (token is ConcreteToken concreteToken)
            {
                // Check if the event is a duplicate by comparing the sequence number (timestamp) with the last processed event
                if (concreteToken.SequenceNumber <= this.state.LastOutcomeEventSequenceNumber)
                {
                    // TODO TODO
                    // OBS currently disabled for analytics actor, since it receives events from many producers all with different sequence numbers
                    // this ruins the purpose of this, since the single actor client cannot distinguish yet between between the producers
                    // It might not work on the other actors either since all consumers may contact all grains, but it happens less
                    // we should fix this pronto
                    // Console.WriteLine($"Duplicate event detected for analytics actor 0");
                    // return Task.CompletedTask;
                }
            }

            // If checkout is successful, update the total sales for the corresponding product
            if (outcome.status == Status.OK)
            {
                var previous = this.state.Query.GetValueOrDefault(outcome.customerId, 0);
                this.state.Query[outcome.customerId] = previous + outcome.total;
            }

            // The request has now been processed fully and we note the sequence number (timestamp) on the token to be able to ignore duplicates later
            if (token is ConcreteToken _concreteToken)
            {
                this.state.LastOutcomeEventSequenceNumber = _concreteToken.SequenceNumber;
            }
            return Task.CompletedTask;
        }

        public async Task<List<KeyValuePair<long, double>>> Top10()
        {
            // Get top ten customers by their value
            var top10 = this.state.Query.OrderByDescending(kv => kv.Value).Take(10).ToList();
            return await Task.FromResult(top10);
        }
    }
}
