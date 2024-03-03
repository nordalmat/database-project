namespace HumanDatabase
{
    public class Block : IBlock
    {
        readonly byte[] sector;
        readonly long?[] cachedHeaderValue = new long?[5];
        readonly Stream stream;
        readonly BlockStorage blockStorage;
        readonly uint id;

        bool isDisposed = false;
        bool isFirstSectorDirty = false;

        public event EventHandler? Disposed;

        public uint Id { get; }

        public Block(BlockStorage storage, uint id, byte[] sector, Stream stream)
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentNullException.ThrowIfNull(sector);
            if (sector.Length != storage.DiskSectorSize)
                throw new ArgumentException("Sector length must be " + storage.DiskSectorSize);
            blockStorage = storage;
            this.id = id;
            this.stream = stream;
            this.sector = sector;
        }

        public long GetHeader(int field)
        {
            if (isDisposed)
                throw new ObjectDisposedException("Block disposed");
            if (field < 0)
                throw new IndexOutOfRangeException();
            if (field >= blockStorage.BlockHeaderSize / 8)
                throw new ArgumentException("Invalid field: " + field);
            if (field < cachedHeaderValue.Length)
            {
                if (cachedHeaderValue[field] == null)
                {
                    cachedHeaderValue[field] = BufferHelper.ReadBufferInt64(sector, field * 8);
                }
                return (long)cachedHeaderValue[field];


            }
            return BufferHelper.ReadBufferInt64(sector, field * 8);
        }

        public void Read(byte[] destination, int destinationOffset, int sourceOffset, int count)
        {
            ObjectDisposedException.ThrowIf(isDisposed, "Block disposed");
            if (false == ((count >= 0) && ((count + sourceOffset) <= blockStorage.BlockContentSize)))
            {
                throw new ArgumentOutOfRangeException("Requested count is outside of src bounds: Count=" + count, "count");
            }

            if (false == ((count + destinationOffset) <= destination.Length))
            {
                throw new ArgumentOutOfRangeException("Requested count is outside of dest bounds: Count=" + count);
            }

            var dataCopied = 0;
            var copyFromFirstSector = (blockStorage.BlockHeaderSize + sourceOffset) < blockStorage.DiskSectorSize;
            if (copyFromFirstSector)
            {
                var tobeCopied = Math.Min(blockStorage.DiskSectorSize - blockStorage.BlockHeaderSize - sourceOffset, count);

                Buffer.BlockCopy(src: sector,
                                    srcOffset: blockStorage.BlockHeaderSize + sourceOffset,
                                    dst: destination,
                                    dstOffset: destinationOffset,
                                    count: tobeCopied);

                dataCopied += tobeCopied;
            }

            if (dataCopied < count)
            {
                if (copyFromFirstSector)
                {
                    stream.Position = (Id * blockStorage.BlockSize) + blockStorage.DiskSectorSize;
                }
                else
                {
                    stream.Position = (Id * blockStorage.BlockSize) + blockStorage.BlockHeaderSize + sourceOffset;
                }
            }

            while (dataCopied < count)
            {
                var bytesToRead = Math.Min(blockStorage.DiskSectorSize, count - dataCopied);
                var thisRead = stream.Read(destination, destinationOffset + dataCopied, bytesToRead);
                if (thisRead == 0)
                {
                    throw new EndOfStreamException();
                }
                dataCopied += thisRead;
            }
        }

        public void SetHeader(int field, long value)
        {
            ObjectDisposedException.ThrowIf(isDisposed, "Block disposed");
            if (field < 0)
            {
                throw new IndexOutOfRangeException();
            }
            if (field < cachedHeaderValue.Length)
            {
                cachedHeaderValue[field] = value;
            }
            BufferHelper.WriteBuffer(value, sector, field * 8);
            isFirstSectorDirty = true;
        }

        public void Write(byte[] source, int sourceOffset, int destinationOffset, int count)
        {
            ObjectDisposedException.ThrowIf(isDisposed, "Block disposed");
            if (false == ((destinationOffset >= 0) && ((destinationOffset + count) <= blockStorage.BlockContentSize)))
            {
                throw new ArgumentOutOfRangeException(nameof(count), "Count argument is outside of dest bounds: Count=" + count);
            }
            if (false == ((sourceOffset >= 0) && ((sourceOffset + count) <= source.Length)))
            {
                throw new ArgumentOutOfRangeException(nameof(count), "Count argument is outside of src bounds: Count=" + count);
            }

            if ((blockStorage.BlockHeaderSize + destinationOffset) < blockStorage.DiskSectorSize)
            {
                var thisWrite = Math.Min(count, blockStorage.DiskSectorSize - blockStorage.BlockHeaderSize - destinationOffset);
                Buffer.BlockCopy(src: source
                    , srcOffset: sourceOffset
                    , dst: sector
                    , dstOffset: blockStorage.BlockHeaderSize + destinationOffset
                    , count: thisWrite);
                isFirstSectorDirty = true;
            }

            if ((blockStorage.BlockHeaderSize + destinationOffset + count) > blockStorage.DiskSectorSize)
            {
                stream.Position = (Id * blockStorage.BlockSize)
                    + Math.Max(blockStorage.DiskSectorSize, blockStorage.BlockHeaderSize + destinationOffset);

                var d = blockStorage.DiskSectorSize - (blockStorage.BlockHeaderSize + destinationOffset);
                if (d > 0)
                {
                    destinationOffset += d;
                    sourceOffset += d;
                    count -= d;
                }

                var written = 0;
                while (written < count)
                {
                    var bytesToWrite = (int)Math.Min(4096, count - written);
                    stream.Write(source, sourceOffset + written, bytesToWrite);
                    stream.Flush();
                    written += bytesToWrite;
                }
            }
        }

        public override string ToString()
        {
            return $"[Block: Id={Id}, ContentLength={GetHeader(2)}, Prev={GetHeader(3)}, Next={GetHeader(0)}]";
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if (disposing & !isDisposed)
            {
                isDisposed = true;
                if (isFirstSectorDirty)
                {
                    stream.Position = Id * blockStorage.BlockSize;
                    stream.Write(sector, 0, 4096);
                    stream.Flush();
                    isFirstSectorDirty = false;
                }
                OnDispose(EventArgs.Empty);

            }
        }

        protected void OnDispose(EventArgs eventArgs)
        {
            if (Disposed != null)
                Disposed(this, eventArgs);
        }

        ~Block()
        {
            Dispose(false);
        }
    }
}