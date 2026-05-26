using ECommerce.Olep.Schema;
using Orleans.Streams;

namespace ECommerce.Olep.Interfaces
{
    public interface IProductActor : IGrainWithIntegerKey
    {
        // accessed by the proxy actor
        Task ProcessInventoryRequest(Inventory inventory, StreamSequenceToken token);

        // not supposed to be acessed by other actors, it is an API for clients
        Task Init(double price, int quantity);

        // accessed by the workload generator only
        Task<double> GetPrice();
        Task<int> GetInventory();
    }
}
