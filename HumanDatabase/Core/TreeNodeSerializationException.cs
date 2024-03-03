namespace HumanDatabase
{
	public class TreeNodeSerializationException(Exception innerException) : Exception("Failed to serialize/deserialize heat map node", innerException)
	{
    }
}