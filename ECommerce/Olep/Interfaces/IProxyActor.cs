namespace ECommerce.Olep.Interfaces
{
    public interface IProxyActor : IGrainWithIntegerKey
    {
        // not supposed to be acessed by other actors, it is an API for clients
        Task Init();
    }
}

