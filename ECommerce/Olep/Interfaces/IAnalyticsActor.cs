namespace ECommerce.Olep.Interfaces
{
    public interface IAnalyticsActor : IGrainWithIntegerKey
    {
        // not supposed to be acessed by other actors, it is an API for clients
        Task Init();

        // accessed by transaction client
        Task<List<KeyValuePair<long, double>>> Top10();
        Task<int> CustomerOutcomeProcessedCount(long customerId);
    }
}
