namespace HumanDatabase
{
    public class Block : IBlock
    {
        public uint Id { get; }

        //
		// Constructors
		//
        


        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public long GetHeader(int field)
        {
            throw new NotImplementedException();
        }

        public void Read(byte[] destination, int destinationOffset, int sourceOffset, int count)
        {
            throw new NotImplementedException();
        }

        public void SetHeader(int field, long value)
        {
            throw new NotImplementedException();
        }

        public void Write(byte[] source, int sourceOffset, int destinationOffset, int count)
        {
            throw new NotImplementedException();
        }
    }
}