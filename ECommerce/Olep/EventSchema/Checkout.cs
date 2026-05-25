using MessagePack;
namespace ECommerce.Olep.Schema
{
    [MessagePackObject]
    public sealed class Checkout
    {
       
        [Key(0)]
        public readonly long productId;
        [Key(1)]
        public readonly long customerId;
        [Key(2)]
        public readonly double price;
        [Key(3)]    
        public readonly int quantity;

        [SerializationConstructor]
        public Checkout(long productId, long customerId, double price, int quantity)
        {
            this.productId = productId;
            this.customerId = customerId;
            this.price = price;
            this.quantity = quantity;
        }

    }
}

