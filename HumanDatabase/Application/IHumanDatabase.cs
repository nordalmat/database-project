namespace Application
{
	public interface IHumanDatabase
	{
		void Insert (HumanModel human);
		void Delete (HumanModel human);
		void Update (HumanModel human);
		HumanModel Find (Guid id);
		IEnumerable<HumanModel> FindBy (string nationality, int age);
	}
}