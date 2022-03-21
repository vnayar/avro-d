/// Avro related exception classes.
module avro.exception;

@safe:

/// Base Avro exception.
class AvroRuntimeException : Exception
{
  this(string msg, Throwable nextInChain = null, string file = __FILE__, size_t line = __LINE__) {
    super(msg, file, line, nextInChain);
  }
}

/// Throw for errors parsing schemas and protocols.
class SchemaParseException : AvroRuntimeException {
  this(string msg, Throwable nextInChain = null, string file = __FILE__, size_t line = __LINE__) {
    super(msg, nextInChain, file, line);
  }
}

/// Thrown when an illegal type is used.
class AvroTypeException : AvroRuntimeException {
  this(string msg, Throwable nextInChain = null, string file = __FILE__, size_t line = __LINE__) {
    super(msg, nextInChain, file, line);
  }
}

/// Thrown when an a decoder detects data that is in an invalid format.
class InvalidNumberEncodingException : AvroRuntimeException {
  this(string msg, Throwable nextInChain = null, string file = __FILE__, size_t line = __LINE__) {
    super(msg, nextInChain, file, line);
  }
}
