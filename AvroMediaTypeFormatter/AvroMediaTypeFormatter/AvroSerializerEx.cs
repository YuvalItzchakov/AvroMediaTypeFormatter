using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.Hadoop.Avro;

namespace AvroMediaTypeFormatter
{
    /// <summary>
    /// A wrapper around Microsoft.Hadoop.Avro implementation.
    /// Capable of serializing / deserializing by runtime-type as opposed to the
    /// AvroSerializer which requires a type at compile time.
    /// </summary>
    public class AvroSerializerEx : IAvroSerializerEx
    {
        private readonly Type serializedType;
        /// <summary>
        /// Initializes a new instance of the <see cref="AvroSerializerEx"/> class.
        /// </summary>
        /// <param name="serializedType">The type to serialize/deserialize to</param>
        /// <exception cref="System.ArgumentNullException">Type cannot be null</exception>
        public AvroSerializerEx(Type serializedType)
        {
            if (serializedType == null)
                throw new ArgumentNullException(nameof(serializedType), "Type cannot be null");

            /* We only check top classes for DataContractAttribute (for efficiency reasons). If there are any nested objects
            that don't decorate themselves with DataContractAttribute, avro will scream */
            if (serializedType.GetCustomAttribute<DataContractAttribute>() == null)
                throw new SerializationException("Cannot serialize/deserialize a type without DataContractAttribute");

            this.serializedType = serializedType;
        }

        private static readonly ConcurrentDictionary<string, Func<byte[], object>> deserializers = new ConcurrentDictionary<string, Func<byte[], object>>();
        private static readonly ConcurrentDictionary<string, Func<object, byte[]>> serializers = new ConcurrentDictionary<string, Func<object, byte[]>>();

        private static readonly MethodInfo avroSerializerCreator = typeof(AvroSerializer).GetMethod("Create", new[] { typeof(AvroSerializerSettings) });

        private static readonly AvroSerializerSettings avroSerializerSettingsParam = new AvroSerializerSettings
        {
            Resolver = new AvroDataContractResolver(true),
            UseCache = true
        };

        /// <summary>
        /// Serializes the specified value.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException"></exception>
        public byte[] Serialize(object value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            if (serializers.ContainsKey(serializedType.Name))
                return serializers[serializedType.Name](value);

            var constructedCreateMethod = avroSerializerCreator.MakeGenericMethod(serializedType);
            var serializerInstance = constructedCreateMethod.Invoke(null, new object[] { avroSerializerSettingsParam });
            var serializeMethod = serializerInstance.GetType().GetMethod("Serialize", new[] { typeof(Stream), serializedType });

            serializers[serializedType.Name] = result =>
            {
                using (var stream = new MemoryStream())
                {
                    serializeMethod.Invoke(serializerInstance, new[] { stream, result });
                    return stream.ToArray();
                }
            };

            return serializers[serializedType.Name](value);
        }

        /// <summary>
        /// Deserializes the specified bytes to deserialize.
        /// </summary>
        /// <param name="bytesToDeserialize">The bytes to deserialize.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException"></exception>
        /// <exception cref="System.InvalidOperationException">Cannot deserialize empty byte array</exception>
        public object Deserialize(byte[] bytesToDeserialize)
        {
            if (bytesToDeserialize == null)
                throw new ArgumentNullException(nameof(bytesToDeserialize));

            if (bytesToDeserialize.Length == 0)
                throw new InvalidOperationException("Cannot deserialize empty byte array");

            if (deserializers.ContainsKey(serializedType.Name))
                return deserializers[serializedType.Name](bytesToDeserialize);

            var constructedCreateMethod = avroSerializerCreator.MakeGenericMethod(serializedType);
            var serializerInstance = constructedCreateMethod.Invoke(null, new object[] { avroSerializerSettingsParam });
            var deserializeMethod = serializerInstance.GetType().GetMethod("Deserialize", new[] { typeof(Stream) });

            deserializers[serializedType.Name] = bytes => deserializeMethod.Invoke(serializerInstance,
                new object[] { new MemoryStream(bytes) });
            return deserializers[serializedType.Name](bytesToDeserialize);
        }
    }
}
