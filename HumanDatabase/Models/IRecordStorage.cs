namespace HumanDatabase
{
    public interface IRecordStorage
    {
        /// <summary>
        /// Updates an record
        /// </summary>
        void Update(uint recordId, byte[] data);

        /// <summary>
        /// Gets records data
        /// </summary>
        byte[] Find(uint recordId);

        /// <summary>
        /// Creates empty record and returns its id
        /// </summary>
        uint Create();

        /// <summary>
        /// Creates new record with given data and returns its id
        /// </summary>
        uint Create(byte[] data);

        /// <summary>
        /// Creates new record with given data generated after the record is allocated and returns its id
        /// </summary>
        uint Create(Func<uint, byte[]> dataGenerator);

        /// <summary>
        /// Deletes a record
        /// </summary>
        void Delete(uint recordId);
    }
    
}