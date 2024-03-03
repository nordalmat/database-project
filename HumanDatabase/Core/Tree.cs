namespace HumanDatabase
{
    public class Tree<K, V> : IIndex<K, V>
    {
        readonly ITreeNodeManager<K, V> nodeManager;
        readonly bool allowDuplicateKeys;

        /// <summary>
        /// Initializes a new instance of the <see cref="Sdb.BTree.Tree`2"/> class.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="nodeManager">Node manager.</param>
        public Tree(ITreeNodeManager<K, V> nodeManager, bool allowDuplicateKeys = false)
        {
            ArgumentNullException.ThrowIfNull(nodeManager);
            this.nodeManager = nodeManager;
            this.allowDuplicateKeys = allowDuplicateKeys;
        }

        /// <summary>
        /// Delete specified entry
        /// </summary>
        public bool Delete(K key, V value, IComparer<V>? valueComparer = null)
        {
            if (false == allowDuplicateKeys)
            {
                throw new InvalidOperationException("This method should be called only from non-unique tree");
            }

            valueComparer = valueComparer == null ? Comparer<V>.Default : valueComparer;

            var deleted = false;
            var shouldContinue = true;

            try
            {
                while (shouldContinue)
                {
                    // Iterating to find all entries we wish to delete
                    using var enumerator = (TreeEnumerator<K, V>)LargerThanOrEqualTo(key).GetEnumerator();
                    while (true)
                    {
                        if (false == enumerator.MoveNext())
                        {
                            shouldContinue = false;
                            break;
                        }
                        var entry = enumerator.Current;
                        if (nodeManager.KeyComparer.Compare(entry.Item1, key) > 0)
                        {
                            shouldContinue = false;
                            break;
                        }
                        if (valueComparer.Compare(entry.Item2, value) == 0)
                        {
                            enumerator.CurrentNode.Remove(enumerator.CurrentEntry);
                            deleted = true;
                            break;
                        }
                    }
                }
            }
            catch (EndEnumeratingException)
            {

            }
            nodeManager.SaveChanges();
            return deleted;
        }

        private class EndEnumeratingException : Exception { }

        /// <summary>
        /// Delete all entries of given key
        /// </summary>
        public bool Delete(K key)
        {
            if (true == allowDuplicateKeys)
            {
                throw new InvalidOperationException("This method should be called only from unique tree");
            }

            using var enumerator = (TreeEnumerator<K, V>)LargerThanOrEqualTo(key).GetEnumerator();
            if (enumerator.MoveNext() && (nodeManager.KeyComparer.Compare(enumerator.Current.Item1, key) == 0))
            {
                enumerator.CurrentNode.Remove(enumerator.CurrentEntry);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Insert an entry to the tree
        /// </summary>
        public void Insert(K key, V value)
        {
            var insertionIndex = 0;
            var leafNode = FindNodeForInsertion(key, ref insertionIndex);

            if (insertionIndex >= 0 && false == allowDuplicateKeys)
            {
                throw new TreeKeyExistsException(key);
            }

            leafNode.InsertAsLeaf(key, value, insertionIndex >= 0 ? insertionIndex : ~insertionIndex);

            if (leafNode.IsOverflow)
            {
                leafNode.Split(out TreeNode<K, V> left, out TreeNode<K, V> right);
            }

            nodeManager.SaveChanges();
        }

        /// <summary>
        /// Find entry by its key, this returns NULL when not foudn
        /// </summary>
        public Tuple<K, V>? Get(K key)
        {
            var insertionIndex = 0;
            var node = FindNodeForInsertion(key, ref insertionIndex);
            if (insertionIndex < 0)
            {
                return null;
            }
            return node.GetEntry(insertionIndex);
        }

        /// <summary>
        /// Search for all elements that larger than or equal to given key
        /// </summary>
        public IEnumerable<Tuple<K, V>> LargerThanOrEqualTo(K key)
        {
            var startIterationIndex = 0;
            var node = Tree<K, V>.FindNodeForIteration(key, nodeManager.RootNode, true, ref startIterationIndex);

            return new TreeTraverser<K, V>(nodeManager
                , node
                , (startIterationIndex >= 0 ? startIterationIndex : ~startIterationIndex) - 1
                , TreeTraverseDirection.Ascending);
        }

        /// <summary>
        /// Search for all elements that larger than given key
        /// </summary>
        public IEnumerable<Tuple<K, V>> LargerThan(K key)
        {
            var startIterationIndex = 0;
            var node = Tree<K, V>.FindNodeForIteration(key, nodeManager.RootNode, false, ref startIterationIndex);

            return new TreeTraverser<K, V>(nodeManager
                , node
                , startIterationIndex >= 0 ? startIterationIndex : (~startIterationIndex - 1)
                , TreeTraverseDirection.Ascending);
        }

        /// <summary>
        /// Search for all elements that is less than or equal to given key
        /// </summary>
        public IEnumerable<Tuple<K, V>> LessThanOrEqualTo(K key)
        {
            var startIterationIndex = 0;
            var node = Tree<K, V>.FindNodeForIteration(key, nodeManager.RootNode, false, ref startIterationIndex);

            return new TreeTraverser<K, V>(nodeManager
                , node
                , startIterationIndex >= 0 ? (startIterationIndex + 1) : ~startIterationIndex
                , TreeTraverseDirection.Decending);
        }

        /// <summary>
        /// Search for all elements that is less than given key
        /// </summary>
        public IEnumerable<Tuple<K, V>> LessThan(K key)
        {
            var startIterationIndex = 0;
            var node = Tree<K, V>.FindNodeForIteration(key, nodeManager.RootNode, true, ref startIterationIndex);

            return new TreeTraverser<K, V>(nodeManager
                , node
                , startIterationIndex >= 0 ? startIterationIndex : ~startIterationIndex
                , TreeTraverseDirection.Decending);
        }

        /// <summary>
        /// Very similar to FindNodeForInsertion(), but this handles the case
        /// where the tree has duplicate keys.
        /// </summary>
        /// <param name="moveLeft">In case of duplicate key found, whenever moving cursor to the left or right</param>
        static TreeNode<K, V> FindNodeForIteration(K key, TreeNode<K, V> node, bool moveLeft, ref int startIterationIndex)
        {
            if (node.IsEmpty)
            {
                startIterationIndex = ~0;
                return node;
            }
            var binarySearchResult = node.BinarySearchEntriesForKey(key, moveLeft ? true : false);
            if (binarySearchResult >= 0)
            {
                if (node.IsLeaf)
                {
                    startIterationIndex = binarySearchResult;
                    return node;
                }
                else
                {
                    return Tree<K, V>.FindNodeForIteration(key, node.GetChildNode(moveLeft ? binarySearchResult : binarySearchResult + 1), moveLeft, ref startIterationIndex);
                }
            }
            else if (false == node.IsLeaf)
            {
                return Tree<K, V>.FindNodeForIteration(key, node.GetChildNode(~binarySearchResult), moveLeft, ref startIterationIndex);
            }
            else
            {
                startIterationIndex = binarySearchResult;
                return node;
            }
        }

        /// <summary>
        /// Search for the node that contains given key, starting from given node
        /// </summary>
        TreeNode<K, V> FindNodeForInsertion(K key, TreeNode<K, V> node, ref int insertionIndex)
        {
            if (node.IsEmpty)
            {
                insertionIndex = ~0;
                return node;
            }

            var binarySearchResult = node.BinarySearchEntriesForKey(key);
            if (binarySearchResult >= 0)
            {
                if (allowDuplicateKeys && false == node.IsLeaf)
                {
                    return FindNodeForInsertion(key, node.GetChildNode(binarySearchResult), ref insertionIndex);
                }
                else
                {
                    insertionIndex = binarySearchResult;
                    return node;
                }
            }
            else if (false == node.IsLeaf)
            {
                return FindNodeForInsertion(key, node.GetChildNode(~binarySearchResult), ref insertionIndex);
            }
            else
            {
                insertionIndex = binarySearchResult;
                return node;
            }
        }

        /// <summary>
        /// SEarch for the node that contains given key, starting from the root node
        /// </summary>
        TreeNode<K, V> FindNodeForInsertion(K key, ref int insertionIndex)
        {
            return FindNodeForInsertion(key, nodeManager.RootNode, ref insertionIndex);
        }
    }
}