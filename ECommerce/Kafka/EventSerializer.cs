using Confluent.Kafka;
using ECommerce.Olep.Schema;
using MessagePack;

namespace ECommerce.Kafka
{
    public class CheckoutEventSerializer : ISerializer<Checkout>, IDeserializer<Checkout>
    {
        public byte[] Serialize(Checkout e, SerializationContext _)
        {
            var data = MessagePackSerializer.Serialize(e);
            //Console.WriteLine($"Serialize event: pId = {e.productId}, price = {e.price}, quantity = {e.quantity}, price = {e.price}, total size = {data.Length}");
            return data;
        }


        public Checkout Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext _)
        {
            if (isNull) return null;
            var e = MessagePackSerializer.Deserialize<Checkout>(data.ToArray());
            //Console.WriteLine($"Deserialize event: pId = {e.productId}, price = {e.price}, quantity = {e.quantity}, price = {e.price}, total size = {data.Length}");
            return e;
        }
    }

    // public class InventoryEventSerializer : ISerializer<Inventory>, IDeserializer<Inventory>
    // {
    //     public byte[] Serialize(Inventory e, SerializationContext _)
    //     {
    //         var data = MessagePackSerializer.Serialize(e);
    //         //Console.WriteLine($"Serialize event: ts = {e.timestamp}, type = {e.type}, content size = {e.content.Length}, total size = {data.Length}");
    //         return data;
    //     }


    //     public Inventory Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext _)
    //     {
    //         if (isNull) return null;
    //         var e = MessagePackSerializer.Deserialize<Inventory>(data.ToArray());
    //         //Console.WriteLine($"Deserialize event: total size = {data.Length}, ts = {e.timestamp}, type = {e.type}, content size = {e.content.Length}");
    //         return e;
    //     }
    // }
}
