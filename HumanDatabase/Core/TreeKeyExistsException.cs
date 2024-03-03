namespace HumanDatabase
{
    public class TreeKeyExistsException(object key) : Exception("Duplicate key: " + key.ToString())
    {
    }

}