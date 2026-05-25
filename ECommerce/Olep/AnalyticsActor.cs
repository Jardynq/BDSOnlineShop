using ECommerce.Olep.Interfaces;
using ECommerce.Olep.Schema;
using Orleans.Concurrency;
using Orleans.Streams;
using Utilities;

namespace ECommerce.Olep
{
    [Reentrant]
    public class AnalyticsActor : Grain, IAnalyticsActor
    {
        private readonly Dictionary<long, double> query;

        public AnalyticsActor()
        {
            this.query = new Dictionary<long, double>();
        }

        // just to force stream subscription
        public Task Init()
        {
            // Clear the query dict between workload runs
            this.query.Clear();

            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            var streamProvider = this.GetStreamProvider(Constants.DefaultStreamProvider);
            var stream = streamProvider.GetStream<Outcome>(Constants.OutcomeNamespace, "0");
            await stream.SubscribeAsync(UpdateAsync);
        }

        // Task 1 implemented here
        private Task UpdateAsync(Outcome outcome, StreamSequenceToken token = null)
        {
            // If checkout is successful, update the total sales for the corresponding product
            if (outcome.status == Status.OK)
            {
                var previous = query.GetValueOrDefault(outcome.productId, 0);
                this.query[outcome.productId] = previous + outcome.total;
            }
            return Task.CompletedTask;
        }

        // Task 1 implemented here
        public async Task<List<KeyValuePair<long, double>>> Top10()
        {
            // Get top ten customers by their value
            var top10 = this.query.OrderByDescending(kv => kv.Value).Take(10).ToList();
            return await Task.FromResult(top10);
        }
    }
}

