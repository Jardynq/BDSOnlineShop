using ECommerce.Olep.Schema;
using Orleans.Streams;

namespace ECommerce.Olep.Interfaces
{
    public interface IAnalyticsActor : IGrainWithIntegerKey
    {
        // accessed by the proxy actor
        Task UpdateAsync(Outcome outcome, StreamSequenceToken token = null);

        // not supposed to be acessed by other actors, it is an API for clients
        Task Init();

        // accessed by transaction client
        Task<List<KeyValuePair<long, double>>> Top10();

        // accessed by transaction client to determine end to end latency.
        // I.e. how long it takes for a checkout request being sent, till the outcome
        // lands at the analytics actor.
        Task<int> CustomerOutcomeProcessedCount(long customerId);
    }
}
