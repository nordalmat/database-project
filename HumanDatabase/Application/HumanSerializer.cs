using HumanDatabase;

namespace Application
{
	/// <summary>
	/// This class serializes a HumanModel into byte[] for using with RecordStorage;
	/// It does not matter how you serialize the model, whenever it is XML, JSON, Protobuf or Binary serialization.
	/// </summary>
	public class HumanSerializer
	{
		public static byte[] Serialize (HumanModel human)
		{
			var nationalityBytes = System.Text.Encoding.UTF8.GetBytes (human.Nationality);
			var nameBytes = System.Text.Encoding.UTF8.GetBytes (human.Nationality);
			var humanData = new byte[
				16 +                   // 16 bytes for Guid id
				4 +                    // 4 bytes indicate the length of `nationality` string
				nationalityBytes.Length +    // n bytes for nationality string
				4 +                    // 4 bytes indicate the length of the `name` string
				nameBytes.Length +     // z bytes for name 
				4 +                    // 4 bytes for age
				4 +                    // 4 bytes indicate length of DNA data
				human.DnaData.Length     // y bytes of DNA data
			];

			Buffer.BlockCopy (
				      src: human.Id.ToByteArray(), 
				srcOffset: 0, 
				      dst: humanData, 
				dstOffset: 0, 
				    count: 16
			);

			Buffer.BlockCopy (
				      src: LittleEndianByteOrder.GetBytes((int)nationalityBytes.Length), 
				srcOffset: 0, 
				      dst: humanData, 
				dstOffset: 16, 
				    count: 4
			);

			Buffer.BlockCopy (
				      src: nationalityBytes, 
				srcOffset: 0, 
				      dst: humanData, 
				dstOffset: 16 + 4, 
				    count: nationalityBytes.Length
			);

			Buffer.BlockCopy (
				      src: LittleEndianByteOrder.GetBytes((int)nameBytes.Length), 
				srcOffset: 0, 
				      dst: humanData, 
				dstOffset: 16 + 4 + nationalityBytes.Length, 
				    count: 4
			);

			Buffer.BlockCopy (
				      src: nameBytes, 
				srcOffset: 0, 
				      dst: humanData, 
				dstOffset: 16 + 4 + nationalityBytes.Length + 4, 
				    count: nameBytes.Length
			);

			Buffer.BlockCopy (
				      src: LittleEndianByteOrder.GetBytes((int)human.Age), 
				srcOffset: 0, 
				      dst: humanData, 
				dstOffset: 16 + 4 + nationalityBytes.Length + 4 + nameBytes.Length, 
				    count: 4
			);

			Buffer.BlockCopy (
				      src: LittleEndianByteOrder.GetBytes(human.DnaData.Length), 
				srcOffset: 0, 
				      dst: humanData, 
				dstOffset: 16 + 4 + nationalityBytes.Length + 4 + nameBytes.Length + 4, 
				    count: 4
			);

			Buffer.BlockCopy (
				      src: human.DnaData, 
				srcOffset: 0, 
				      dst: humanData, 
				dstOffset: 16 + 4 + nationalityBytes.Length + 4 + nameBytes.Length + 4 + 4, 
				    count: human.DnaData.Length
			);

			return humanData;
		}

		public static HumanModel Deserializer (byte[] data)
		{
			var humanModel = new HumanModel();

			humanModel.Id = BufferHelper.ReadBufferGuid (data, 0);

			var nationalityLength = BufferHelper.ReadBufferInt32 (data, 16);
			if (nationalityLength < 0 || nationalityLength > (16*1024)) {
				throw new Exception ("Invalid string length: " + nationalityLength);
			}
			humanModel.Nationality = System.Text.Encoding.UTF8.GetString (data, 16 + 4, nationalityLength);

			var nameLength = BufferHelper.ReadBufferInt32 (data, 16 + 4 + nationalityLength);
			if (nameLength < 0 || nameLength > (16*1024)) {
				throw new Exception ("Invalid string length: " + nameLength);
			}
			humanModel.Name = System.Text.Encoding.UTF8.GetString (data, 16 + 4 + nationalityLength + 4, nameLength);

			humanModel.Age = BufferHelper.ReadBufferInt32 (data, 16 + 4 + nationalityLength + 4 + nameLength);

			var dnaLength = BufferHelper.ReadBufferInt32 (data, 16 + 4 + nationalityLength + 4 + nameLength + 4);
			if (dnaLength < 0 || dnaLength > (64*1024)) {
				throw new Exception ("Invalid DNA data length: " + dnaLength);
			}
			humanModel.DnaData = new byte[dnaLength];
			Buffer.BlockCopy (src: data, srcOffset: 16 + 4 + nationalityLength + 4 + nameLength + 4 + 4, dst: humanModel.DnaData, dstOffset: 0, count: humanModel.DnaData.Length);
			return humanModel;
		}
	}
}