namespace HumanDatabase
{
    public interface IBlockStorage
    {
        /// <summary>
        /// Total number of bytes of custom data ber block
        /// </summary>
        int BlockContentSize { get; }

        /// <summary>
        /// Total number of bytes in header
        /// </summary>
        int BlockHeaderSize { get; }

        /// <summary>
        /// Total size of the block in bytes, equals to the sum of BlockContentSize and BlockHeaderSize
        /// </summary>
        int BlockSize { get; }

        /// <summary>
        /// Find a block by its id
        /// </summary>
        IBlock Find(uint blockId);

        /// <summary>
        /// Create new block, extend the length of the storage
        /// </summary>
        IBlock CreateNew ();
    }
    
}