namespace HumanDatabase
{
    public class BlockStorage : IBlockStorage
    {
        readonly Stream stream;
        readonly int blockSize;
        readonly int blockHeaderSize;
		readonly int blockContentSize;
        readonly int unitOfWork;
        readonly Dictionary<uint, Block> blocks = new Dictionary<uint, Block> ();

        public int UnitOfWork { get; }
        public int BlockContentSize { get; }
        public int BlockHeaderSize { get; }
        public int BlockSize { get; }

    	//
		// Constructors
		//

        public BlockStorage(Stream storage, int blockSize = 40960, int blockHeaderSize = 48)
        {
            if (storage == null)
                throw new ArgumentNullException("Storage stream is null");
            if (blockHeaderSize >= blockSize) {
				throw new ArgumentException ("Block header size cannot be larger than or equal to Block size");
			}
            if (blockSize < 128) 
				throw new ArgumentException ("Block size is too small, must be larger than or equal to 128");

            this.blockSize = blockSize;
            this.blockHeaderSize = blockHeaderSize;
            this.blockContentSize = blockSize - blockHeaderSize;
            this.stream = storage;
        }

        public IBlock CreateNew()
        {
            throw new NotImplementedException();
        }

        public IBlock Find(uint blockId)
        {
            throw new NotImplementedException();
        }
    }

}