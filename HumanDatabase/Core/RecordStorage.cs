namespace HumanDatabase
{
    /// <summary>
    /// Record storage service that store data in form of records, each
    /// record made up from one or several blocks
    /// </summary>
    public class RecordStorage : IRecordStorage
    {
        readonly IBlockStorage storage;

        const int MaxRecordSize = 4194304; // 4MB
        const int kNextBlockId = 0;
        const int kRecordLength = 1;
        const int kBlockContentLength = 2;
        const int kPreviousBlockId = 3;
        const int kIsDeleted = 4;

        public RecordStorage(IBlockStorage storage)
        {
            ArgumentNullException.ThrowIfNull(storage);

            this.storage = storage;

            if (storage.BlockHeaderSize < 48)
            {
                throw new ArgumentException("Record storage needs at least 48 header bytes");
            }
        }

        public virtual byte[]? Find(uint recordId)
        {
            using var block = storage.Find(recordId);
            if (block == null)
            {
                return null;
            }

            if (1L == block.GetHeader(kIsDeleted))
            {
                return null;
            }

            if (0L != block.GetHeader(kPreviousBlockId))
            {
                return null;
            }

            var totalRecordSize = block.GetHeader(kRecordLength);
            if (totalRecordSize > MaxRecordSize)
            {
                throw new NotSupportedException("Unexpected record length: " + totalRecordSize);
            }
            var data = new byte[totalRecordSize];
            var bytesRead = 0;

            IBlock currentBlock = block;
            while (true)
            {
                uint nextBlockId;

                using (currentBlock)
                {
                    var thisBlockContentLength = currentBlock.GetHeader(kBlockContentLength);
                    if (thisBlockContentLength > storage.BlockContentSize)
                    {
                        throw new InvalidDataException("Unexpected block content length: " + thisBlockContentLength);
                    }

                    currentBlock.Read(destination: data, destinationOffset: bytesRead, sourceOffset: 0, count: (int)thisBlockContentLength);

                    bytesRead += (int)thisBlockContentLength;

                    nextBlockId = (uint)currentBlock.GetHeader(kNextBlockId);
                    if (nextBlockId == 0)
                    {
                        return data;
                    }
                }

                currentBlock = storage.Find(nextBlockId);
                if (currentBlock == null)
                {
                    throw new InvalidDataException("Block not found by id: " + nextBlockId);
                }
            }
        }

        public virtual uint Create(Func<uint, byte[]> dataGenerator)
        {
            if (dataGenerator == null)
            {
                throw new ArgumentException();
            }

            using var firstBlock = AllocateBlock();
            var returnId = firstBlock.Id;

            var data = dataGenerator(returnId);
            var dataWritten = 0;
            var dataTobeWritten = data.Length;
            firstBlock.SetHeader(kRecordLength, dataTobeWritten);

            if (dataTobeWritten == 0)
            {
                return returnId;
            }

            IBlock currentBlock = firstBlock;
            while (dataWritten < dataTobeWritten)
            {
                IBlock? nextBlock = null;

                using (currentBlock)
                {
                    var thisWrite = (int)Math.Min(storage.BlockContentSize, dataTobeWritten - dataWritten);
                    currentBlock.Write(data, dataWritten, 0, thisWrite);
                    currentBlock.SetHeader(kBlockContentLength, thisWrite);
                    dataWritten += thisWrite;

                    if (dataWritten < dataTobeWritten)
                    {
                        nextBlock = AllocateBlock();
                        var success = false;
                        try
                        {
                            nextBlock.SetHeader(kPreviousBlockId, currentBlock.Id);
                            currentBlock.SetHeader(kNextBlockId, nextBlock.Id);
                            success = true;
                        }
                        finally
                        {
                            if ((false == success) && (nextBlock != null))
                            {
                                nextBlock.Dispose();
                                nextBlock = null;
                            }
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                if (nextBlock != null)
                {
                    currentBlock = nextBlock;
                }
            }
            return returnId;
        }

        public virtual uint Create(byte[] data)
        {
            if (data == null)
            {
                throw new ArgumentException();
            }

            return Create(recordId => data);
        }

        public virtual uint Create()
        {
            using var firstBlock = AllocateBlock();
            return firstBlock.Id;
        }

        public virtual void Delete(uint recordId)
        {
            using var block = storage.Find(recordId);
            IBlock? currentBlock = block;
            while (true)
            {
                IBlock? nextBlock = null;

                using (currentBlock)
                {
                    MarkAsFree(currentBlock.Id);
                    currentBlock.SetHeader(kIsDeleted, 1L);

                    var nextBlockId = (uint)currentBlock.GetHeader(kNextBlockId);
                    if (nextBlockId == 0)
                    {
                        break;
                    }
                    else
                    {
                        nextBlock = storage.Find(nextBlockId);
                        if (currentBlock == null)
                        {
                            throw new InvalidDataException("Block not found by id: " + nextBlockId);
                        }
                    }
                }

                if (nextBlock != null)
                {
                    currentBlock = nextBlock;
                }
            }
        }

        public virtual void Update(uint recordId, byte[] data)
        {
            var written = 0;
            var total = data.Length;
            var blocks = FindBlocks(recordId);
            var blocksUsed = 0;
            var previousBlock = (IBlock)null;

            try
            {
                while (written < total)
                {
                    var bytesToWrite = Math.Min(total - written, storage.BlockContentSize);
                    var blockIndex = (int)Math.Floor((double)written / (double)storage.BlockContentSize);

                    var target = (IBlock)null;
                    if (blockIndex < blocks.Count)
                    {
                        target = blocks[blockIndex];
                    }
                    else
                    {
                        target = AllocateBlock();
                        if (target == null)
                        {
                            throw new Exception("Failed to allocate new block");
                        }
                        blocks.Add(target);
                    }

                    if (previousBlock != null)
                    {
                        previousBlock.SetHeader(kNextBlockId, target.Id);
                        target.SetHeader(kPreviousBlockId, previousBlock.Id);
                    }

                    target.Write(source: data, sourceOffset: written, destinationOffset: 0, count: bytesToWrite);
                    target.SetHeader(kBlockContentLength, bytesToWrite);
                    target.SetHeader(kNextBlockId, 0);
                    if (written == 0)
                    {
                        target.SetHeader(kRecordLength, total);
                    }

                    blocksUsed++;
                    written += bytesToWrite;
                    previousBlock = target;
                }

                if (blocksUsed < blocks.Count)
                {
                    for (var i = blocksUsed; i < blocks.Count; i++)
                    {
                        MarkAsFree(blocks[i].Id);
                    }
                }
            }
            finally
            {
                foreach (var block in blocks)
                {
                    block.Dispose();
                }
            }
        }

        /// <summary>
        /// Find all blocks of given record, return these blocks in order.
        /// </summary>
        /// <param name="recordId">Record identifier.</param>
        List<IBlock> FindBlocks(uint recordId)
        {
            var blocks = new List<IBlock>();
            var success = false;

            try
            {
                var currentBlockId = recordId;

                do
                {
                    var block = storage.Find(currentBlockId);
                    if (null == block)
                    {
                        if (currentBlockId == 0)
                        {
                            block = storage.CreateNew();
                        }
                        else
                        {
                            throw new Exception("Block not found by id: " + currentBlockId);
                        }
                    }
                    blocks.Add(block);

                    if (1L == block.GetHeader(kIsDeleted))
                    {
                        throw new InvalidDataException("Block not found: " + currentBlockId);
                    }

                    currentBlockId = (uint)block.GetHeader(kNextBlockId);
                } while (currentBlockId != 0);

                success = true;
                return blocks;
            }
            finally
            {
                if (false == success)
                {
                    foreach (var block in blocks)
                    {
                        block.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Allocate new block for use, either by dequeueing an exising non-used block
        /// or creating a new one
        /// </summary>
        /// <returns>Newly allocated block ready to use.</returns>
        IBlock AllocateBlock()
        {
            IBlock newBlock;
            if (false == TryFindFreeBlock(out uint resuableBlockId))
            {
                newBlock = storage.CreateNew();
                if (newBlock == null)
                {
                    throw new Exception("Failed to create new block");
                }
            }
            else
            {
                newBlock = storage.Find(resuableBlockId);
                if (newBlock == null)
                {
                    throw new InvalidDataException("Block not found by id: " + resuableBlockId);
                }
                newBlock.SetHeader(kBlockContentLength, 0L);
                newBlock.SetHeader(kNextBlockId, 0L);
                newBlock.SetHeader(kPreviousBlockId, 0L);
                newBlock.SetHeader(kRecordLength, 0L);
                newBlock.SetHeader(kIsDeleted, 0L);
            }
            return newBlock;
        }

        bool TryFindFreeBlock(out uint blockId)
        {
            blockId = 0;
            GetSpaceTrackingBlock(out IBlock lastBlock, out IBlock secondLastBlock);

            using (lastBlock)
            using (secondLastBlock)
            {
                var currentBlockContentLength = lastBlock.GetHeader(kBlockContentLength);
                if (currentBlockContentLength == 0)
                {
                    if (secondLastBlock == null)
                    {
                        return false;
                    }

                    blockId = ReadUInt32FromTrailingContent(secondLastBlock);

                    secondLastBlock.SetHeader(kBlockContentLength, secondLastBlock.GetHeader(kBlockContentLength) - 4);
                    AppendUInt32ToContent(secondLastBlock, lastBlock.Id);

                    secondLastBlock.SetHeader(kBlockContentLength, secondLastBlock.GetHeader(kBlockContentLength) + 4);
                    secondLastBlock.SetHeader(kNextBlockId, 0);
                    lastBlock.SetHeader(kPreviousBlockId, 0);
                    return true;
                }
                else
                {
                    blockId = ReadUInt32FromTrailingContent(lastBlock);
                    lastBlock.SetHeader(kBlockContentLength, currentBlockContentLength - 4);
                    return true;
                }
            }
        }

        static void AppendUInt32ToContent(IBlock block, uint value)
        {
            var contentLength = block.GetHeader(kBlockContentLength);

            if ((contentLength % 4) != 0)
            {
                throw new DataMisalignedException("Block content length not %4: " + contentLength);
            }

            block.Write(source: LittleEndianByteOrder.GetBytes(value), sourceOffset: 0, destinationOffset: (int)contentLength, count: 4);
        }

        static uint ReadUInt32FromTrailingContent(IBlock block)
        {
            var buffer = new byte[4];
            var contentLength = block.GetHeader(kBlockContentLength);

            if ((contentLength % 4) != 0)
            {
                throw new DataMisalignedException("Block content length not %4: " + contentLength);
            }

            if (contentLength == 0)
            {
                throw new InvalidDataException("Trying to dequeue UInt32 from an empty block");
            }

            block.Read(destination: buffer, destinationOffset: 0, sourceOffset: (int)contentLength - 4, count: 4);
            return LittleEndianByteOrder.GetUInt32(buffer);
        }

        void MarkAsFree(uint blockId)
        {
            IBlock? targetBlock = null;
            GetSpaceTrackingBlock(out IBlock lastBlock, out IBlock secondLastBlock);

            using (lastBlock)
            using (secondLastBlock)
            {
                try
                {
                    var contentLength = lastBlock.GetHeader(kBlockContentLength);
                    if ((contentLength + 4) <= storage.BlockContentSize)
                    {
                        targetBlock = lastBlock;
                    }
                    else
                    {
                        targetBlock = storage.CreateNew();
                        targetBlock.SetHeader(kPreviousBlockId, lastBlock.Id);

                        lastBlock.SetHeader(kNextBlockId, targetBlock.Id);

                        contentLength = 0;
                    }
                    AppendUInt32ToContent(targetBlock, blockId);
                    targetBlock.SetHeader(kBlockContentLength, contentLength + 4);
                }
                finally
                {
                    if (targetBlock != null)
                    {
                        targetBlock.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Get the last 2 blocks from the free space tracking record, 
        /// </summary>
        void GetSpaceTrackingBlock(out IBlock lastBlock, out IBlock secondLastBlock)
        {
            lastBlock = null;
            secondLastBlock = null;

            var blocks = FindBlocks(0);

            try
            {
                if (blocks == null || (blocks.Count == 0))
                {
                    throw new Exception("Failed to find blocks of record 0");
                }
                lastBlock = blocks[blocks.Count - 1];
                if (blocks.Count > 1)
                {
                    secondLastBlock = blocks[blocks.Count - 2];
                }
            }
            finally
            {
                if (blocks != null)
                {
                    foreach (var block in blocks)
                    {
                        if ((lastBlock == null || block != lastBlock)
                            && (secondLastBlock == null || block != secondLastBlock))
                        {
                            block.Dispose();
                        }
                    }
                }
            }
        }
    }
}