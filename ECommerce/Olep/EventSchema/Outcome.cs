namespace ECommerce.Olep.Schema
{
    [Serializable]
    public sealed class Outcome
    {
        public readonly long customerId;
        public readonly long productId;
        public readonly double total;
        public readonly Status status;

        public Outcome(long customerId, long productId, double total, Status status)
        {
            this.customerId = customerId;
            this.productId = productId;
            this.total = total;
            this.status = status;
        }
    }
}
