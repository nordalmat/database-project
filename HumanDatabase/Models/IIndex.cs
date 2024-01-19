namespace HumanDatabase
{
    public interface IIndex<K, V>
    {
        /// <summary>
        /// Create new entry in this index that maps key K to value V
        /// </summary>
        void Insert(K key, V value);

        /// <summary>
        /// Get an entry by key
        /// </summary>
        Tuple<K, V> Get(K key);

        /// <summary>
        /// Find all entries that contain a key larger than or equal specified key
        /// </summary>
        IEnumerable<Tuple<K, V>> LargerThanOrEqualTo(K key);

        /// <summary>
        /// Find all entries that contain a key larger than specified key
        /// </summary>
        IEnumerable<Tuple<K, V>> LargerThan (K key);

        /// <summary>
        /// Find all entries that contain a key less than or equal specified key
        /// </summary>
        IEnumerable<Tuple<K, V>> LessThanOrEqualTo (K key);

        /// <summary>
        /// Find all entries that contain a key less than specified key
        /// </summary>
        IEnumerable<Tuple<K, V>> LessThan (K key);

        /// <summary>
        /// Delete an entry from the index, optionally use specified IComparer to compare values
        /// </summary>
        bool Delete(K key, V value, IComparer<V>? valueComparer = null);

        /// <summary>
        /// Delete all entries of given key
        /// </summary>
        bool Delete(K key);
    }
}