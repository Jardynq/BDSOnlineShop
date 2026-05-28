using ECommerce.Olep.Checkpointing;
using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using ECommerce.Olep.Token;
using MessagePack;
using Orleans.Streams;
using System.Text.Json;
using Utilities;

namespace ECommerce.Olep
{
    [MessagePackObject]
    public class AnalyticsActorState : ICloneable
    {
        [Key(0)]
        public Dictionary<long, double> Query { get; set; }

        // Use a dictionary of hashSets so each Kafka Partition is a key into the the dictionary
        // Then we store the last Constants.StoreCapacity offsets from this partition to see if they have been processed before
        // This is the "Sliding window technique" and it is necessary for the analytics actor since this grain can be activate from different consumers using the same partition
        [Key(1)]
        public Dictionary<int, HashSet<long>> ProcessedOutcomeEvent { get; set; }

        [Key(2)]
        public Dictionary<long, int> DebugQuery { get; set; }

        public object Clone()
        {
            string temp = JsonSerializer.Serialize(this);
            return JsonSerializer.Deserialize<AnalyticsActorState>(temp);
        }

        public AnalyticsActorState()
        {
            this.Query = new Dictionary<long, double>();
            this.DebugQuery = new Dictionary<long, int>();
            this.ProcessedOutcomeEvent = new Dictionary<int, HashSet<long>>(Constants.StoreCapacity);
        }
    }

    //[Reentrant]
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

            if (token is KafkaSequenceToken concreteToken)
            {
                // Check if the event is a duplicate by comparing the sequence number (timestamp) with the last processed event
                if (this.state.ProcessedOutcomeEvent.TryGetValue(concreteToken.EventIndex, out HashSet<long> set))
                {
                    if (set.Contains(concreteToken.SequenceNumber))
                    {
                        // Already processed sequence number from this partition so we discard the processing of this event and continue
                        Console.WriteLine($"Duplicate event detected for analytics actor 0: partition = {concreteToken.EventIndex}, offset = {concreteToken.SequenceNumber}");
                        return Task.CompletedTask;
                    }
                }
            }

            // If checkout is successful, update the total sales for the corresponding product
            if (outcome.status == Status.OK)
            {
                var previous = this.state.Query.GetValueOrDefault(outcome.customerId, 0);
                this.state.Query[outcome.customerId] = previous + outcome.total;
            }

            // Increment the debug counter for end-to-end latency metrics.
            var previousCount = this.state.DebugQuery.GetValueOrDefault(outcome.customerId, 0);
            this.state.DebugQuery[outcome.customerId] = previousCount + 1;

            // The request has now been processed fully and we note the sequence number (timestamp) on the token to be able to ignore duplicates later
            if (token is KafkaSequenceToken _concreteToken)
            {
                if (state.ProcessedOutcomeEvent.ContainsKey(_concreteToken.EventIndex))
                {

                    while (state.ProcessedOutcomeEvent[_concreteToken.EventIndex].Count() >= Constants.StoreCapacity)
                    {
                        long oldestOffset = state.ProcessedOutcomeEvent[_concreteToken.EventIndex].Min();
                        state.ProcessedOutcomeEvent[_concreteToken.EventIndex].Remove(oldestOffset);
                    }
                    state.ProcessedOutcomeEvent[_concreteToken.EventIndex].Add(_concreteToken.SequenceNumber);
                }
                else
                {
                    state.ProcessedOutcomeEvent.Add(
                        _concreteToken.EventIndex,
                        new HashSet<long>(Constants.StoreCapacity)
                        {
                            _concreteToken.SequenceNumber
                        }
                    );
                }
            }
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

        public async Task<double> GetSumOfAllBalance()
        {
            return await Task.FromResult(this.state.Query.Values.Sum());
        }
    }
}
