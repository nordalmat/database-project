using System.Collections;

namespace HumanDatabase
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Sdb.BTree.TreeEnumerator`2"/> class.
    /// </summary>
    /// <param name="nodeManager">Node manager.</param>
    /// <param name="node">Node.</param>
    /// <param name="fromIndex">From index.</param>
    /// <param name="direction">Direction.</param>
    public class TreeEnumerator<K, V>(ITreeNodeManager<K, V> nodeManager, TreeNode<K, V> node, int fromIndex, TreeTraverseDirection direction) : IEnumerator<Tuple<K, V>>
    {
        readonly ITreeNodeManager<K, V> nodeManager = nodeManager;
        readonly TreeTraverseDirection direction = direction;

        bool doneIterating = false;
        int currentEntry = fromIndex;
        TreeNode<K, V> currentNode = node;

        Tuple<K, V> current;

        public TreeNode<K, V> CurrentNode
        { get { return currentNode; } }

        public int CurrentEntry
        { get { return currentEntry; } }

        object IEnumerator.Current
        { get { return Current; } }

        public Tuple<K, V> Current { get { return current; } }

        public bool MoveNext()
        {
            if (doneIterating)
            {
                return false;
            }

            return direction switch
            {
                TreeTraverseDirection.Ascending => MoveForward(),
                TreeTraverseDirection.Decending => MoveBackward(),
                _ => throw new ArgumentOutOfRangeException(),
            };
        }

        bool MoveForward()
        {
            if (currentNode.IsLeaf)
            {
                currentEntry++;

                while (true)
                {
                    if (currentEntry < currentNode.EntriesCount)
                    {
                        current = currentNode.GetEntry(currentEntry);
                        return true;
                    }
                    else if (currentNode.ParentId != 0)
                    {
                        currentEntry = currentNode.IndexInParent();
                        currentNode = nodeManager.Find(currentNode.ParentId);
                        if ((currentEntry < 0) || (currentNode == null))
                        {
                            throw new Exception("Something gone wrong with the BTree");
                        }
                    }
                    else
                    {
                        current = null;
                        doneIterating = true;
                        return false;
                    }
                }
            }
            else
            {
                currentEntry++;
                do
                {
                    currentNode = currentNode.GetChildNode(currentEntry);
                    currentEntry = 0;
                } while (false == currentNode.IsLeaf);

                current = currentNode.GetEntry(currentEntry);
                return true;
            }
        }

        bool MoveBackward()
        {
            if (currentNode.IsLeaf)
            {
                currentEntry--;

                while (true)
                {
                    if (currentEntry >= 0)
                    {
                        current = currentNode.GetEntry(currentEntry);
                        return true;
                    }
                    else if (currentNode.ParentId != 0)
                    {
                        currentEntry = currentNode.IndexInParent() - 1;
                        currentNode = nodeManager.Find(currentNode.ParentId);

                        if (currentNode == null)
                        {
                            throw new Exception("Something gone wrong with the BTree");
                        }
                    }
                    else
                    {
                        doneIterating = true;
                        current = null;
                        return false;
                    }
                }
            }
            else
            {
                do
                {
                    currentNode = currentNode.GetChildNode(currentEntry);
                    currentEntry = currentNode.EntriesCount;
                    if ((currentEntry < 0) || (currentNode == null))
                    {
                        throw new Exception("Something gone wrong with the BTree");
                    }
                } while (false == currentNode.IsLeaf);

                currentEntry -= 1;
                current = currentNode.GetEntry(currentEntry);
                return true;
            }
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
        }
    }
}