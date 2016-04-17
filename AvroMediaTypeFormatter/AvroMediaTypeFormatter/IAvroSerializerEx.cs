namespace AvroMediaTypeFormatter
{
    public interface IAvroSerializerEx
    {
        byte[] Serialize(object value);
        object Deserialize(byte[] bytesToDeserialize);
    }
}