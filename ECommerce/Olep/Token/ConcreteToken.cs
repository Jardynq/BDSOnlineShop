using Orleans.Streams;

namespace ECommerce.Olep.Token
{
    public class ConcreteToken : StreamSequenceToken
    {
        public override long SequenceNumber { get; protected set ; }

        public override int EventIndex { get; protected set ; }

        public ConcreteToken(long sequenceNumber, int eventIndex)
        {
            this.SequenceNumber = sequenceNumber;
            this.EventIndex = eventIndex;
        }

        public override int CompareTo(StreamSequenceToken other)
        {
            int seq = this.SequenceNumber.CompareTo(other.SequenceNumber);
            if (seq != 0)
            {
                return seq;
            }
            else
            {
                return this.EventIndex.CompareTo(other.EventIndex);
            }
        }

        public override bool Equals(StreamSequenceToken other)
        {
            return this.SequenceNumber == other.SequenceNumber && this.EventIndex == other.EventIndex;
        }
    }
}
