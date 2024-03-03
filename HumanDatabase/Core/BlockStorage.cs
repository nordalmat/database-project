namespace HumanDatabase
{
    public class BlockStorage : IBlockStorage
    {
        readonly Stream stream;
        readonly int blockSize;
        readonly int blockHeaderSize;
        readonly int blockContentSize;
        readonly int diskSectorSize;
        readonly Dictionary<uint, Block> blocks = new Dictionary<uint, Block>();

        public int DiskSectorSize { get; }
        public int BlockContentSize { get; }
        public int BlockHeaderSize { get; }
        public int BlockSize { get; }

        public BlockStorage(Stream storage, int blockSize = 40960, int blockHeaderSize = 48)
        {
            ArgumentNullException.ThrowIfNull(storage);
            if (blockHeaderSize >= blockSize)
            {
                throw new ArgumentException("Block header size cannot be larger than or equal to Block size");
            }
            if (blockSize < 128)
                throw new ArgumentException("Block size is too small, must be larger than or equal to 128");

            diskSectorSize = blockSize >= 4096 ? 4096 : 128;
            this.blockSize = blockSize;
            this.blockHeaderSize = blockHeaderSize;
            blockContentSize = blockSize - blockHeaderSize;
            stream = storage;
        }

        public IBlock CreateNew()
        {
            if (stream.Length % blockSize != 0)
                throw new DataMisalignedException("Unexpected length of the stream: " + stream.Length);

            uint blockId = (uint)Math.Ceiling((double)stream.Length / blockSize);
            stream.SetLength(blockId * blockSize + blockSize);
            stream.Flush();

            Block block = new Block(this, blockId, new byte[DiskSectorSize], stream);
            InitializeBlock(block);
            return block;
        }

        public IBlock? Find(uint blockId)
        {
            if (blocks.TryGetValue(blockId, out Block? value))
                return value;

            long blockPosition = blockId * blockSize;
            if (blockPosition + blockSize > stream.Length)
                return null;

            stream.Position = blockPosition;
            byte[] leftPointer = new byte[DiskSectorSize];
            stream.Read(leftPointer, 0, DiskSectorSize);

            Block block = new Block(this, blockId, leftPointer, stream);
            InitializeBlock(block);
            return block;
        }

        protected void InitializeBlock(Block block)
        {
            blocks[block.Id] = block;
            block.Disposed += HandleBlockDispose;
        }

        protected void HandleBlockDispose(object? sender, EventArgs eventArgs)
        {
            if (sender != null) 
            {
                Block block = (Block)sender;
                block.Disposed -= HandleBlockDispose;
                blocks.Remove(block.Id);
            }
        }
    }

}