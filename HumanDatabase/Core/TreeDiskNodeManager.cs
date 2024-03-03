namespace HumanDatabase
{
    public sealed class TreeDiskNodeManager<K, V> : ITreeNodeManager<K, V>
    {
        readonly IRecordStorage recordStorage;
        readonly Dictionary<uint, TreeNode<K, V>> dirtyNodes = new Dictionary<uint, TreeNode<K, V>>();
        readonly Dictionary<uint, WeakReference<TreeNode<K, V>>> nodeWeakRefs = new Dictionary<uint, WeakReference<TreeNode<K, V>>>();
        readonly Queue<TreeNode<K, V>> nodeStrongRefs = new Queue<TreeNode<K, V>>();
        readonly int maxStrongNodeRefs = 200;
        readonly TreeDiskNodeSerializer<K, V> serializer;
        readonly ushort minEntriesPerNode = 36;

        TreeNode<K, V> rootNode;
        int cleanupCounter = 0;

        public ushort MinEntriesPerNode { get { return minEntriesPerNode; } }

        public IComparer<Tuple<K, V>> EntryComparer { get; private set; }

        public IComparer<K> KeyComparer
        { get; private set; }

        public TreeNode<K, V> RootNode
        { get { return rootNode; } }

        /// <summary>
        /// Construct a tree from given storage, using default comparer of key
        /// </summary>
        public TreeDiskNodeManager(ISerializer<K> keySerializer, ISerializer<V> valueSerializer, IRecordStorage nodeStorage) : this(keySerializer, valueSerializer, nodeStorage, Comparer<K>.Default)
        {
        }

        /// <summary>
        /// Construct a tree from given storage, using the specified comparer of key
        /// </summary>
        /// <param name="keySerializer">Tool to serialize node keys.</param>
        /// <param name="valueSerializer">Tool to serialize node values<param>
        /// <param name="recordStorage">Underlying tool for storage.</param>
        /// <param name="keyComparer">Key comparer.</param>
        public TreeDiskNodeManager(ISerializer<K> keySerializer, ISerializer<V> valueSerializer, IRecordStorage recordStorage, IComparer<K> keyComparer)
        {
            ArgumentNullException.ThrowIfNull(recordStorage);

            this.recordStorage = recordStorage;
            serializer = new TreeDiskNodeSerializer<K, V>(this, keySerializer, valueSerializer);
            KeyComparer = keyComparer;
            EntryComparer = Comparer<Tuple<K, V>>.Create((a, b) =>
            {
                return KeyComparer.Compare(a.Item1, b.Item1);
            });

            var firstBlockData = recordStorage.Find(1u);
            if (firstBlockData != null)
            {
                rootNode = Find(BufferHelper.ReadBufferUInt32(firstBlockData, 0));
            }
            else
            {
                rootNode = CreateFirstRoot();
            }
        }

        public TreeNode<K, V> Create(IEnumerable<Tuple<K, V>> entries, IEnumerable<uint> childrenIds)
        {
            TreeNode<K, V>? node = null;
            recordStorage.Create(nodeId =>
            {
                node = new TreeNode<K, V>(this, nodeId, 0, entries, childrenIds);
                OnNodeInitialized(node);
                return serializer.Serialize(node);
            });

            if (node == null)
            {
                throw new Exception("dataGenerator never called by nodeStorage");
            }

            return node;
        }

        public TreeNode<K, V> Find(uint id)
        {
            if (nodeWeakRefs.ContainsKey(id))
            {
                if (nodeWeakRefs[id].TryGetTarget(out TreeNode<K, V> node))
                {
                    return node;
                }
                else
                {
                    nodeWeakRefs.Remove(id);
                }
            }
            var data = recordStorage.Find(id);
            if (data == null)
            {
                return null;
            }
            var dNode = serializer.Deserialize(id, data);
            OnNodeInitialized(dNode);
            return dNode;
        }

        public TreeNode<K, V> CreateNewRoot(K key, V value, uint leftNodeId, uint rightNodeId)
        {
            var node = Create([
                new Tuple<K, V> (key, value)
            ], [
                leftNodeId,
                rightNodeId
            ]);
            rootNode = node;
            recordStorage.Update(1u, LittleEndianByteOrder.GetBytes(node.Id));
            return rootNode;
        }

        public void MakeRoot(TreeNode<K, V> node)
        {
            rootNode = node;
            recordStorage.Update(1u, LittleEndianByteOrder.GetBytes(node.Id));
        }

        public void Delete(TreeNode<K, V> node)
        {
            if (node == rootNode)
            {
                rootNode = null;
            }

            recordStorage.Delete(node.Id);

            if (dirtyNodes.ContainsKey(node.Id))
            {
                dirtyNodes.Remove(node.Id);
            }
        }

        public void MarkAsChanged(TreeNode<K, V> node)
        {
            if (false == dirtyNodes.ContainsKey(node.Id))
            {
                dirtyNodes.Add(node.Id, node);
            }
        }

        public void SaveChanges()
        {
            foreach (var kv in dirtyNodes)
            {
                recordStorage.Update(kv.Value.Id, serializer.Serialize(kv.Value));
            }

            dirtyNodes.Clear();
        }

        TreeNode<K, V> CreateFirstRoot()
        {
            recordStorage.Create(LittleEndianByteOrder.GetBytes((uint)2));
            return Create(null, null);
        }

        void OnNodeInitialized(TreeNode<K, V> node)
        {
            nodeWeakRefs.Add(node.Id, new WeakReference<TreeNode<K, V>>(node));
            nodeStrongRefs.Enqueue(node);
            if (nodeStrongRefs.Count >= maxStrongNodeRefs)
            {
                while (nodeStrongRefs.Count >= (maxStrongNodeRefs / 2f))
                {
                    nodeStrongRefs.Dequeue();
                }
            }

            if (cleanupCounter++ >= 1000)
            {
                cleanupCounter = 0;
                var tobeDeleted = new List<uint>();
                foreach (var kv in nodeWeakRefs)
                {
                    if (false == kv.Value.TryGetTarget(out TreeNode<K, V> target))
                    {
                        tobeDeleted.Add(kv.Key);
                    }
                }

                foreach (var key in tobeDeleted)
                {
                    nodeWeakRefs.Remove(key);
                }
            }
        }
    }
}