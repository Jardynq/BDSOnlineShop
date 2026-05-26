namespace ECommerce.Olep.Interfaces
{
    public interface IKafkaCheckoutProxyActor : IGrainWithIntegerKey
    {
        Task StartFromBootstrap();
    }

    public interface IKafkaInventoryProxyActor : IGrainWithIntegerKey
    {
        Task StartFromBootstrap();
    }

    public interface IKafkaOutcomeProxyActor : IGrainWithIntegerKey
    {
        Task StartFromBootstrap();
    }
}
