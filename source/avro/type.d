module avro.type;

/**
   Each AvroSchema has a type, with complex types allowing a schema to contain other schemas of
   the same or different types.
*/
public enum Type {
  // [Primitive Types](https://avro.apache.org/docs/current/spec.html#schema_primitive)
  NULL,    /// no value
  BOOLEAN, /// a binary value
  INT,     /// 32-bit signed integer
  LONG,    /// 64-bit signed integer
  FLOAT,   /// single precision (32-bit) IEEE 754 floating-point number
  DOUBLE,  /// double precision (64-bit) IEEE 754 floating-point number
  BYTES,   /// sequence of 8-bit unsigned bytes
  STRING,  /// unicode character sequence
  /// [Complex Types](https://avro.apache.org/docs/current/spec.html#schema_complex)
  RECORD,  /// A record contains several fields, each with a name and type.
  ENUM,    /// An enumerated list of named values.
  ARRAY,   /// A list of values which all have the same type.
  MAP,     /// An associative array from a string name to values with a given type.
  UNION,   /// A value with has one of several possible types.
  FIXED,   /// A numerical value with a fixed size in bytes.
}

/**
   A mapping from a type's name, e.g. "float" or "int" to the primitive Schema Type.

   Primitive type names may not be defined in any namespace.
*/
public enum Type[string] PRIMITIVE_TYPE_BY_NAME = [
    "null": Type.NULL,
    "boolean": Type.BOOLEAN,
    "int": Type.INT,
    "long": Type.LONG,
    "float": Type.FLOAT,
    "double": Type.DOUBLE,
    "bytes": Type.BYTES,
    "string": Type.STRING
];
