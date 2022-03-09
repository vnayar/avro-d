# avro-d

An implementation of the [Apache Avro](https://avro.apache.org/docs/current/) serialization
framework in the D Programming Language.

Apache Avro provides:
- Rich data structures
- A compact, fast, binary data format.
- A container file, to store persistent data.
- Remote procedure call (RPC).
- Simple integration with dynamic languages. Code generation is not required to read or write data
  files nor to use or implement RPC protocols. Code generation as an optional optimization, only
  worth implementing for statically typed languages.

The Apache Avro specification is significantly more complex than other data serialization formats
such as [Google Protocol Buffers](https://developers.google.com/protocol-buffers). The full [Apache
Avro Specification](https://avro.apache.org/docs/current/spec.html) provides more details.

## Features Implemented

### Schema representation

A set of data classes exist to represent schemas for generation in code or the processing of data.

For example:
```d
import avro.schema;
auto schema = new UnionSchema([
    Schema.createPrimitive(Type.STRING),
    Schema.createPrimitive(Type.INT)]);
```

### Schema parsing & validation

Schemas may be parsed from files, text, or JSON.

For example:
```d
import avro.parser;
auto parser = new Parser();
Schema schema =  parser.parseText(q"EOS
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number", "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}
EOS");
```

Errors in the JSON format of a schema will lead to descriptive errors.

### Generic Data types

Generic data objects may be created according to schemas with their values set to schema-appropriate
defaults and validation logic when setting values. Most `GenericDatum` objects make use of
`.getValue!T()` and `.setValue(T)(T val)` methods, however, many convenience functions also exist.

For example:
```d
import avro.generic.genericdata;

// Initializes the GenericDatum according to the schema with default values.
GenericDatum datum = new GenericDatum(schema);
assert(datum.getType == Type.RECORD);

// Primitive values can be set and retrieved.
datum.getValue!(GenericRecord).getField("name").setValue("bob");

// Convenience shortcut using opIndex() and opAssign() for primitive types.
datum["name"] = "bob";

assert(datum["name"].getValue!string == "bob");

// Enums have convenience functions directly on GenericData.
datum["favorite_number"].setUnionIndex(0);
assert(datum["favorite_number"].getUnionIndex() == 0);

// Arrays also have convenience functions.
datum["scores"] ~= 1.23f;
datum["scores"] ~= 4.56f;
assert(datum["scores"].length == 2);
p
// Maps do as well.
datum["m"]["m1"] = 10L;
datum["m"]["m2"] = 20L;
assert(datum["m"]["m1"].getValue!long == 10L);
```

### Binary Serialization/Deserialization

`GenericData` objects can be written using an encoder.

For example:
```d
import avro.codec.binaryencoder;
import avro.generic.genericwriter;

ubyte[] data;
auto encoder = binaryEncoder(appender(&data));
GenericWriter writer = new GenericWriter(schema, encoder);
writer.write(datum);

assert(data == [
// Field: name
// len=3     b     o     b
    0x06, 0x62, 0x6F, 0x62,
// Field: favorite_number
// idx=0     8
    0x00, 0x10,
// Field: favorite_color
// idx=0 len=4     b     l     u     e
    0x00, 0x08, 0x62, 0x6C, 0x75, 0x65
]);
```

They may also be read using a decoder.

For example:
```d
import avro.codec.binarydecoder;
import avro.generic.genericreader;

auto decoder = binaryDecoder(data);
GenericReader reader = new GenericReader(schema, decoder);
GenericDatum datum;
reader.read(datum);

assert(datum["name"].getValue!string() == "bob");
assert(datum["favorite_number"].getValue!int() == 8);
assert(datum["favorite_color"].getValue!string() == "blue");
```

## Features Not Yet Implemented

- Logical Type support
- Specific Data types generated for schemas
- JSON Serialization/Deserialization
- Codex compression support
- Object Container Files
- Protocol wire format
- Schema Resolution
