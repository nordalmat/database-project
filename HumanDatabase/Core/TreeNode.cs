using System.Diagnostics;

namespace HumanDatabase
{
    public class TreeNode<K, V>
    {
        protected uint id = 0;
        protected uint parentId;
        protected readonly ITreeNodeManager<K, V> nodeManager;
        protected readonly List<uint> childrenIds;
        protected readonly List<Tuple<K, V>> entries;

        public K MaxKey
        { get { return entries[entries.Count - 1].Item1; }
        }

        public K MinKey
        { get { return entries[0].Item1; }
        }

        public bool IsEmpty
        { get { return entries.Count == 0; }
        }

        public bool IsLeaf
        { get { return childrenIds.Count == 0; }
        }

        public bool IsOverflow
        { get { return entries.Count > (nodeManager.MinEntriesPerNode * 2); }
        }

        public int EntriesCount
        { get { return entries.Count; }
        }

        public int ChildrenNodeCount
        { get { return childrenIds.Count; }
        }

        public uint ParentId
        { get { return parentId; } private set { parentId = value; nodeManager.MarkAsChanged(this); }
        }

        public uint[] ChildrenIds
        { get { return childrenIds.ToArray(); }
        }

        public Tuple<K, V>[] Entries
        { get { return entries.ToArray(); }
        }

        /// <summary>
        /// Id of this node, assigned by node manager. Node never change its id itself
        /// </summary>
        public uint Id
        { get { return id; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Sdb.BTree.Node`2"/> class.
        /// </summary>
        /// <param name="branchingFactor">Branching factor.</param>
        /// <param name="nodeManager">Node manager.</param>
        public TreeNode(ITreeNodeManager<K, V> nodeManager, uint id, uint parentId, IEnumerable<Tuple<K, V>> entries = null, IEnumerable<uint> childrenIds = null)
        {
            ArgumentNullException.ThrowIfNull(nodeManager);

            this.id = id;
            this.parentId = parentId;
            this.nodeManager = nodeManager;
            this.childrenIds = new List<uint>();
            this.entries = new List<Tuple<K, V>>(this.nodeManager.MinEntriesPerNode * 2);

            if (entries != null)
            {
                this.entries.AddRange(entries);
            }

            if (childrenIds != null)
            {
                this.childrenIds.AddRange(childrenIds);
            }
        }


        /// <summary>
        /// Remove an entry from this instance. 
        /// </summary>
        public void Remove(int removeAt)
        {
            if (false == (removeAt >= 0) && (removeAt < this.entries.Count))
            {
                throw new ArgumentOutOfRangeException();
            }

            if (IsLeaf)
            {
                entries.RemoveAt(removeAt);
                nodeManager.MarkAsChanged(this);

                if ((EntriesCount >= nodeManager.MinEntriesPerNode) || (parentId == 0))
                {
                    return;
                }
                else
                {
                    Rebalance();
                }
            }
            else
            {
                var leftSubTree = nodeManager.Find(this.childrenIds[removeAt]);
                leftSubTree.FindLargest(out TreeNode<K, V> largestNode, out int largestIndex);
                var replacementEntry = largestNode.GetEntry(largestIndex);

                this.entries[removeAt] = replacementEntry;
                nodeManager.MarkAsChanged(this);

                largestNode.Remove(largestIndex);
            }
        }

        /// <summary>
        /// Get this node's index in its parent
        /// </summary>
        public int IndexInParent()
        {
            var parent = nodeManager.Find(parentId) ?? throw new Exception("IndexInParent fails to find parent node of " + id);
            var childrenIds = parent.ChildrenIds;
            for (var i = 0; i < childrenIds.Length; i++)
            {
                if (childrenIds[i] == id)
                {
                    return i;
                }
            }

            throw new Exception("Failed to find index of node " + id + " in its parent");
        }

        /// <summary>
        /// Find the largest entry on this subtree and output it to specified parameters
        /// </summary>
        public void FindLargest(out TreeNode<K, V> node, out int index)
        {
            if (IsLeaf)
            {
                node = this;
                index = this.entries.Count - 1;
                return;
            }
            else
            {
                var rightMostNode = nodeManager.Find(this.childrenIds[this.childrenIds.Count - 1]);
                rightMostNode.FindLargest(out node, out index);
            }
        }

        /// <summary>
        /// Find the smallest entry on this subtree and output it to specified parameters
        /// </summary>
        public void FindSmallest(out TreeNode<K, V> node, out int index)
        {
            if (IsLeaf)
            {
                node = this;
                index = 0;
                return;
            }
            else
            {
                var leftMostNode = nodeManager.Find(this.childrenIds[0]);
                leftMostNode.FindSmallest(out node, out index);
            }
        }

        public void InsertAsLeaf(K key, V value, int insertPosition)
        {
            Debug.Assert(IsLeaf, "Call this method on leaf node only");

            entries.Insert(insertPosition, new Tuple<K, V>(key, value));
            nodeManager.MarkAsChanged(this);
        }

        public void InsertAsParent(K key, V value, uint leftReference, uint rightReference, out int insertPosition)
        {
            Debug.Assert(false == IsLeaf, "Call this method on non-leaf node only");

            insertPosition = BinarySearchEntriesForKey(key);
            insertPosition = insertPosition >= 0 ? insertPosition : ~insertPosition;
            entries.Insert(insertPosition, new Tuple<K, V>(key, value));
            childrenIds.Insert(insertPosition, leftReference);
            childrenIds[insertPosition + 1] = rightReference;
            nodeManager.MarkAsChanged(this);
        }

        /// <summary>
        /// Split this node in half
        /// </summary>
        public void Split(out TreeNode<K, V> outLeftNode, out TreeNode<K, V> outRightNode)
        {
            Debug.Assert(IsOverflow, "Calling Split when node is not overflow");

            var halfCount = this.nodeManager.MinEntriesPerNode;
            var middleEntry = entries[halfCount];
            var rightEntries = new Tuple<K, V>[halfCount];
            var rightChildren = (uint[])null;
            entries.CopyTo(halfCount + 1, rightEntries, 0, rightEntries.Length);
            if (false == IsLeaf)
            {
                rightChildren = new uint[halfCount + 1];
                childrenIds.CopyTo(halfCount + 1, rightChildren, 0, rightChildren.Length);
            }
            var newRightNode = nodeManager.Create(rightEntries, rightChildren);

            if (rightChildren != null)
            {
                foreach (var childId in rightChildren)
                {
                    nodeManager.Find(childId).ParentId = newRightNode.Id;
                }
            }
            entries.RemoveRange(halfCount);

            if (false == IsLeaf)
            {
                childrenIds.RemoveRange(halfCount + 1);
            }
            var parent = parentId == 0 ? null : nodeManager.Find(parentId);
            if (parent == null)
            {
                parent = this.nodeManager.CreateNewRoot(middleEntry.Item1, middleEntry.Item2, id, newRightNode.Id);
                this.ParentId = parent.Id;
                newRightNode.ParentId = parent.Id;
            }
            else
            {
                parent.InsertAsParent(middleEntry.Item1, middleEntry.Item2, id, newRightNode.Id, out int insertPosition);
                newRightNode.ParentId = parent.id;
                if (parent.IsOverflow)
                {
                    parent.Split(out TreeNode<K, V> left, out TreeNode<K, V> right);
                }
            }
            outLeftNode = this;
            outRightNode = newRightNode;
            nodeManager.MarkAsChanged(this);
        }

        /// <summary>
        /// Perform a binary search on entries
        /// </summary>
        public int BinarySearchEntriesForKey(K key)
        {
            return entries.BinarySearch(new Tuple<K, V>(key, default), this.nodeManager.EntryComparer);
        }

        /// <summary>
        /// Perform binary search on entries, but if there are multiple occurences,
        /// return either last or first occurence based on firstOccurrence param
        /// </summary>
        /// <param name="firstOccurence">If set to <c>true</c> first occurence.</param>
        public int BinarySearchEntriesForKey(K key, bool firstOccurence)
        {
            if (firstOccurence)
            {
                return entries.BinarySearchFirst(new Tuple<K, V>(key, default), this.nodeManager.EntryComparer);
            }
            else
            {
                return entries.BinarySearchLast(new Tuple<K, V>(key, default), this.nodeManager.EntryComparer);
            }
        }

        /// <summary>
        /// Get a children node by its internal position to this node
        /// </summary>
        public TreeNode<K, V> GetChildNode(int atIndex)
        {
            return nodeManager.Find(childrenIds[atIndex]);
        }

        /// <summary>
        /// Get a Key-Value entry inside this node
        /// </summary>
        public Tuple<K, V> GetEntry(int atIndex)
        {
            return entries[atIndex];
        }

        /// <summary>
        /// Check if there is an entry at given index
        /// </summary>
        public bool EntryExists(int atIndex)
        {
            return atIndex < entries.Count;
        }

        public override string ToString()
        {
            if (IsLeaf)
            {
                var numbers = (from tuple in this.entries select tuple.Item1.ToString()).ToArray();
                return string.Format("[Node: Id={0}, ParentId={1}, Entries={2}]", Id, ParentId, String.Join(",", numbers));
            }
            else
            {
                var numbers = (from tuple in this.entries select tuple.Item1.ToString()).ToArray();
                var ids = (from id in this.childrenIds select id.ToString()).ToArray();
                return string.Format("[Node: Id={0}, ParentId={1}, Entries={2}, Children={3}]", Id, ParentId, String.Join(",", numbers), String.Join(",", ids));
            }
        }

        /// <summary>
        /// Rebalance this node after an element has been removed causing it to underflow
        /// </summary>
        void Rebalance()
        {
            var indexInParent = IndexInParent();
            var parent = nodeManager.Find(parentId);
            var rightSibling = ((indexInParent + 1) < parent.ChildrenNodeCount) ? parent.GetChildNode(indexInParent + 1) : null;
            if ((rightSibling != null) && (rightSibling.EntriesCount > nodeManager.MinEntriesPerNode))
            {
                entries.Add(parent.GetEntry(indexInParent));
                parent.entries[indexInParent] = rightSibling.entries[0];
                rightSibling.entries.RemoveAt(0);
                if (false == rightSibling.IsLeaf)
                {
                    var n = nodeManager.Find(rightSibling.childrenIds[0]);
                    n.parentId = this.id;
                    nodeManager.MarkAsChanged(n);
                    childrenIds.Add(rightSibling.childrenIds[0]);
                    rightSibling.childrenIds.RemoveAt(0);
                }
                nodeManager.MarkAsChanged(this);
                nodeManager.MarkAsChanged(parent);
                nodeManager.MarkAsChanged(rightSibling);
                return;
            }
            var leftSibling = ((indexInParent - 1) >= 0) ? parent.GetChildNode(indexInParent - 1) : null;
            if ((leftSibling != null) && (leftSibling.EntriesCount > nodeManager.MinEntriesPerNode))
            {
                entries.Insert(0, parent.GetEntry(indexInParent - 1));
                parent.entries[indexInParent - 1] = leftSibling.entries[leftSibling.entries.Count - 1];
                leftSibling.entries.RemoveAt(leftSibling.entries.Count - 1);
                if (false == IsLeaf)
                {
                    var n = nodeManager.Find(leftSibling.childrenIds[leftSibling.childrenIds.Count - 1]);
                    n.parentId = this.id;
                    nodeManager.MarkAsChanged(n);
                    childrenIds.Insert(0, leftSibling.childrenIds[leftSibling.childrenIds.Count - 1]);
                    leftSibling.childrenIds.RemoveAt(leftSibling.childrenIds.Count - 1);
                }
                nodeManager.MarkAsChanged(this);
                nodeManager.MarkAsChanged(parent);
                nodeManager.MarkAsChanged(leftSibling);
                return;
            }
            var leftChild = rightSibling != null ? this : leftSibling;
            var rightChild = rightSibling ?? this;
            var seperatorParentIndex = rightSibling != null ? indexInParent : (indexInParent - 1);
            leftChild.entries.Add(parent.GetEntry(seperatorParentIndex));

            leftChild.entries.AddRange(rightChild.entries);
            leftChild.childrenIds.AddRange(rightChild.childrenIds);
            foreach (var id in rightChild.childrenIds)
            {
                var n = nodeManager.Find(id);
                n.parentId = leftChild.id;
                nodeManager.MarkAsChanged(n); ;
            }
            parent.entries.RemoveAt(seperatorParentIndex);
            parent.childrenIds.RemoveAt(seperatorParentIndex + 1);
            nodeManager.Delete(rightChild);
            if (parent.parentId == 0 && parent.EntriesCount == 0)
            {
                leftChild.parentId = 0;
                nodeManager.MarkAsChanged(leftChild);
                nodeManager.MakeRoot(leftChild);
                nodeManager.Delete(parent); 
            }
            else if ((parent.parentId != 0) && (parent.EntriesCount < nodeManager.MinEntriesPerNode))
            {
                nodeManager.MarkAsChanged(leftChild); 
                nodeManager.MarkAsChanged(parent);
                parent.Rebalance();
            }
            else
            {
                nodeManager.MarkAsChanged(leftChild);
                nodeManager.MarkAsChanged(parent);
            }
        }
    }
}