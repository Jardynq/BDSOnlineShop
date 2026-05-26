namespace ECommerce.Olep.Interfaces
{
    public interface IKafkaCheckoutProxyActor : IGrainWithIntegerKey
    {
        Task StartConsumingAsync();
    }

    public interface IKafkaInventoryProxyActor : IGrainWithIntegerKey
    {
        Task StartConsumingAsync();
    }

    public interface IKafkaOutcomeProxyActor : IGrainWithIntegerKey
    {
        Task StartConsumingAsync();
    }
}
