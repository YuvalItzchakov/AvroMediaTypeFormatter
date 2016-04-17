# AvroMediaTypeFormatter
A media type formatter for Avro serialization with WebAPI

A MediaTypeFormatter for Avro. Uses the media type "application/avro" to invoke the media type formatter.
Contains a wrapper around Microsoft.Hadoop.Avro which allows to work with runtime types.

Add it to the global configuration inside the `WebApiConfig.Register` method:

```c#
config.Formatters.Add(new AvroMediaTypeFormatter());
```
