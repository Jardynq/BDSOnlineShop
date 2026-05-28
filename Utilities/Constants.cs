namespace Utilities
{
    public class Constants
    {
        public const int SiloPort = 11111;
        public const int GatewayPort = 30000;
        public const string ClusterId = "LocalTestCluster";
        public const string ServiceId = "BDSOnlineShop";

        public const string CheckoutNamespace = "checkout";
        public const string CustomerNamespace = "customer";
        public const string InventoryNamespace = "inventory";
        public const string OutcomeNamespace = "BD713788-B5AE-49FF-8B2C-F311B9CB0CA0";

        public const string DefaultStreamProvider = "SMSProvider";

        public const string CheckoutTopicGroup = "checkout-group";
        public const string InventoryTopicGroup = "inventory-group";
        public const string OutcomeTopicGroup = "outcome-group";

        public const string kafkaService = "localhost:9092";
        public const string zooKeeperService = "localhost:2181";

        public const int StoreCapacity = 128;
    }
}
