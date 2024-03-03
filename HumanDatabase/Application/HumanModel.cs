namespace Application
{
	/// <summary>
	/// Our database stores humans, first we define our Human model
	/// </summary>
	public class HumanModel
	{
		public Guid Id { get; set; }

		public string Nationality { get; set; }

		public int Age { get; set; }

		public string Name { get; set; }

		public byte[] DnaData { get; set; }

		public override string ToString ()
		{
			return string.Format ("[HumanModel: Id={0}, Nationality={1}, Age={2}, Name={3}, DnaData={4}]", Id, Nationality, Age, Name, DnaData.Length + " bytes");
		}
	}
}