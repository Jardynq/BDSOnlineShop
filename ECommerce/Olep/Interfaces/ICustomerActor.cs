using ECommerce.Olep.Schema;
using Orleans.Streams;

namespace ECommerce.Olep.Interfaces
{
    public interface ICustomerActor : IGrainWithIntegerKey
    {
        // accessed by the proxy actor
        Task ProcessCheckout(Checkout checkout, StreamSequenceToken token = null);
        Task ProcessOutcome(Outcome outcome, StreamSequenceToken token = null);

        // not supposed to be acessed by other actors, it is an API for clients
        Task Init(double balance);

        // accessed by the workload generator only
        Task<double> GetBalance();

    }
}
