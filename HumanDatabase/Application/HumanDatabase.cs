using HumanDatabase;

namespace Application
{
	/// <summary>
	/// Then, define our database
	/// </summary>
	class HumanDatabase : IDisposable
	{
		readonly Stream mainDatabaseFile;
		readonly Stream primaryIndexFile;
		readonly Stream secondaryIndexFile;
		readonly Tree<Guid, uint> primaryIndex;
		readonly Tree<Tuple<string, int>, uint> secondaryIndex;
		readonly RecordStorage humanRecords;
		readonly HumanSerializer humanSerializer = new HumanSerializer ();

		/// <summary>
		/// </summary>
		/// <param name="pathToHumanDb">Path to human db.</param>
		public HumanDatabase (string pathToHumanDb)	
		{
            ArgumentNullException.ThrowIfNull(pathToHumanDb);

            this.mainDatabaseFile = new FileStream (pathToHumanDb, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096);
			this.primaryIndexFile = new FileStream (pathToHumanDb + ".pidx", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096);
			this.secondaryIndexFile = new FileStream (pathToHumanDb + ".sidx", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096);
			this.humanRecords = new RecordStorage (new BlockStorage(this.mainDatabaseFile, 4096, 48));
			this.primaryIndex = new Tree<Guid, uint> (
				new TreeDiskNodeManager<Guid, uint>(
					new GuidSerializer(),
					new TreeUIntSerializer(),
					new RecordStorage(new BlockStorage(this.primaryIndexFile, 4096))
				),
				false
			);

			this.secondaryIndex = new Tree<Tuple<string, int>, uint> (
				new TreeDiskNodeManager<Tuple<string, int>, uint>(
					new StringIntSerializer(), 
					new TreeUIntSerializer(), 
					new RecordStorage(new BlockStorage(this.secondaryIndexFile, 4096))
				),
				true
			);
		}

		/// <summary>
		/// Update given human
		/// </summary>
		public void Update (HumanModel human)
		{
			if (disposed) {
                throw new ObjectDisposedException("HumanDatabase");
			}

			throw new NotImplementedException ();
		}

		/// <summary>
		/// Insert a new human entry into our human database
		/// </summary>
		public void Insert (HumanModel human)
		{
			if (disposed) {
				throw new ObjectDisposedException ("HumanDatabase");
			}
			var recordId = this.humanRecords.Create (HumanSerializer.Serialize(human));
			this.primaryIndex.Insert (human.Id, recordId);
			this.secondaryIndex.Insert (new Tuple<string, int>(human.Nationality, human.Age), recordId);
		}

		/// <summary>
		/// Find a human by its unique id
		/// </summary>
		public HumanModel Find (Guid humanId)
		{
			if (disposed) {
				throw new ObjectDisposedException ("HumanDatabase");
			}
			var entry = this.primaryIndex.Get (humanId);
			if (entry == null) {
				return null;
			}
			return HumanSerializer.Deserializer(this.humanRecords.Find (entry.Item2));
		}

		/// <summary>
		/// Find all humans that belongs to given nationality and age
		/// </summary>
		public IEnumerable<HumanModel> FindBy (string nationality, int age)
		{
			var comparer = Comparer<Tuple<string, int>>.Default;
			var searchKey = new Tuple<string, int>(nationality, age);
			foreach (var entry in this.secondaryIndex.LargerThanOrEqualTo (searchKey))
			{
				if (comparer.Compare(entry.Item1, searchKey) > 0) {
					break;
				}
				yield return HumanSerializer.Deserializer(this.humanRecords.Find (entry.Item2));
			}
		}

		/// <summary>
		/// Delete specified human from our database
		/// </summary>
		public void Delete (HumanModel human)
		{
			throw new NotImplementedException ();
		}

		#region Dispose
		public void Dispose ()
		{
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		bool disposed = false;

		protected virtual void Dispose (bool disposing)
		{
			if (disposing && !disposed)
			{
				this.mainDatabaseFile.Dispose ();
				this.secondaryIndexFile.Dispose();
				this.primaryIndexFile.Dispose ();
				this.disposed = true;
			}
		}

		~HumanDatabase() 
		{
			Dispose (false);
		}
		#endregion
	}
}