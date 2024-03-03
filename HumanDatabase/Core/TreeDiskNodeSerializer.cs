namespace HumanDatabase
{
    public sealed class TreeDiskNodeSerializer<K, V>
    {
        readonly ISerializer<K> keySerializer;
        readonly ISerializer<V> valueSerializer;
        readonly ITreeNodeManager<K, V> nodeManager;

        /// <summary>
        /// Construct a tree serializer that uses given key/value serializers 
        /// </summary>
        public TreeDiskNodeSerializer(ITreeNodeManager<K, V> nodeManager, ISerializer<K> keySerializer, ISerializer<V> valueSerializer)
        {
            ArgumentNullException.ThrowIfNull(nodeManager);
            ArgumentNullException.ThrowIfNull(valueSerializer);
            ArgumentNullException.ThrowIfNull(keySerializer);

            this.nodeManager = nodeManager;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        /// <summary>
        /// Serialize given node into byte array that will then be written down to RecordStorage
        /// </summary>
        public byte[] Serialize(TreeNode<K, V> node)
        {
            if (keySerializer.IsFixedSize && valueSerializer.IsFixedSize)
            {
                return FixedLengthSerialize(node);
            }
            else if (valueSerializer.IsFixedSize)
            {
                return VariableKeyLengthSerialize(node);
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Deserialize given node from data which is read from RecordStorage
        /// </summary>
        public TreeNode<K, V> Deserialize(uint assignId, byte[] record)
        {
            if (keySerializer.IsFixedSize && valueSerializer.IsFixedSize)
            {
                return FixedLengthDeserialize(assignId, record);
            }
            else if (valueSerializer.IsFixedSize)
            {
                return VariableKeyLengthDeserialize(assignId, record);
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        byte[] FixedLengthSerialize(TreeNode<K, V> node)
        {
            var entrySize = keySerializer.Length + valueSerializer.Length;
            var size = 16
                + node.Entries.Length * entrySize
                + node.ChildrenIds.Length * 4;
            if (size >= (1024 * 64))
            {
                throw new Exception("Serialized node size too large: " + size);
            }
            var buffer = new byte[size];
            BufferHelper.WriteBuffer(node.ParentId, buffer, 0);
            BufferHelper.WriteBuffer((uint)node.EntriesCount, buffer, 4);
            BufferHelper.WriteBuffer((uint)node.ChildrenNodeCount, buffer, 8);

            for (var i = 0; i < node.EntriesCount; i++)
            {
                var entry = node.GetEntry(i);
                Buffer.BlockCopy(keySerializer.Serialize(entry.Item1), 0, buffer, 12 + i * entrySize, keySerializer.Length);
                Buffer.BlockCopy(valueSerializer.Serialize(entry.Item2), 0, buffer, 12 + i * entrySize + keySerializer.Length, valueSerializer.Length);
            }

            var childrenIds = node.ChildrenIds;
            for (var i = 0; i < node.ChildrenNodeCount; i++)
            {
                BufferHelper.WriteBuffer(childrenIds[i], buffer, 12 + entrySize * node.EntriesCount + (i * 4));
            }
            return buffer;
        }

        TreeNode<K, V> FixedLengthDeserialize(uint assignId, byte[] buffer)
        {
            var entrySize = keySerializer.Length + valueSerializer.Length;
            var parentId = BufferHelper.ReadBufferUInt32(buffer, 0);
            var entriesCount = BufferHelper.ReadBufferUInt32(buffer, 4);
            var childrenCount = BufferHelper.ReadBufferUInt32(buffer, 8);

            var entries = new Tuple<K, V>[entriesCount];
            for (var i = 0; i < entriesCount; i++)
            {
                var key = keySerializer.Deserialize(buffer
                    , 12 + i * entrySize
                    , keySerializer.Length);
                var value = valueSerializer.Deserialize(buffer
                    , 12 + i * entrySize + keySerializer.Length
                    , valueSerializer.Length);
                entries[i] = new Tuple<K, V>(key, value);
            }

            var children = new uint[childrenCount];
            for (var i = 0; i < childrenCount; i++)
            {
                children[i] = BufferHelper.ReadBufferUInt32(buffer, (int)(12 + entrySize * entriesCount + (i * 4)));
            }
            return new TreeNode<K, V>(nodeManager, assignId, parentId, entries, children);
        }

        TreeNode<K, V> VariableKeyLengthDeserialize(uint assignId, byte[] buffer)
        {
            var parentId = BufferHelper.ReadBufferUInt32(buffer, 0);
            var entriesCount = BufferHelper.ReadBufferUInt32(buffer, 4);
            var childrenCount = BufferHelper.ReadBufferUInt32(buffer, 8);

            var entries = new Tuple<K, V>[entriesCount];
            var p = 12;
            for (var i = 0; i < entriesCount; i++)
            {
                var keyLength = BufferHelper.ReadBufferInt32(buffer, p);
                var key = keySerializer.Deserialize(buffer, p + 4, keyLength);
                var value = valueSerializer.Deserialize(buffer, p + 4 + keyLength, valueSerializer.Length);
                entries[i] = new Tuple<K, V>(key, value);
                p += 4 + keyLength + valueSerializer.Length;
            }

            var children = new uint[childrenCount];
            for (var i = 0; i < childrenCount; i++)
            {
                children[i] = BufferHelper.ReadBufferUInt32(buffer, (int)(p + (i * 4)));
            }
            return new TreeNode<K, V>(nodeManager, assignId, parentId, entries, children);
        }

        byte[] VariableKeyLengthSerialize(TreeNode<K, V> node)
        {
            using var m = new MemoryStream();
            m.Write(LittleEndianByteOrder.GetBytes((uint)node.ParentId), 0, 4);
            m.Write(LittleEndianByteOrder.GetBytes((uint)node.EntriesCount), 0, 4);
            m.Write(LittleEndianByteOrder.GetBytes((uint)node.ChildrenNodeCount), 0, 4);

            for (var i = 0; i < node.EntriesCount; i++)
            {
                var entry = node.GetEntry(i);
                var key = keySerializer.Serialize(entry.Item1);
                var value = valueSerializer.Serialize(entry.Item2);
                m.Write(LittleEndianByteOrder.GetBytes((int)key.Length), 0, 4);
                m.Write(key, 0, key.Length);
                m.Write(value, 0, value.Length);
            }
            var childrenIds = node.ChildrenIds;
            for (var i = 0; i < node.ChildrenNodeCount; i++)
            {
                m.Write(LittleEndianByteOrder.GetBytes((uint)childrenIds[i]), 0, 4);
            }
            return m.ToArray();
        }
    }
}