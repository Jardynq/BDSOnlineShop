namespace ECommerce.Olep.Schema
{
    [Serializable]
    public sealed class Checkout
    {
        public readonly long productId;
        public readonly double price;
        public readonly int quantity;

        public Checkout(long productId, double price, int quantity)
        {
            this.productId = productId;
            this.price = price;
            this.quantity = quantity;
        }
    }
}
