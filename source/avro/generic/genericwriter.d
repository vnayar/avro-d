module avro.generic.genericwriter;

import std.conv : to;
import avro.generic.genericdata :
    GenericDatum, GenericFixed, GenericRecord, GenericEnum, GenericArray, GenericUnion, GenericMap;
import avro.exception : AvroRuntimeException, AvroTypeException;
import avro.type : Type;
import avro.schema : Schema;
import avro.codec.datumwriter : DatumWriter;
import avro.codec.encoder : Encoder;

/// [DatumWriter] for GenericDatum objects.
class GenericWriter {
  private Schema schema;
  private Encoder encoder;

  /// Uses a given encoder to convert a [GenericDatum] into its serialized format.
  static void write(GenericDatum datum, Encoder e) {
    if (datum.isUnion()) {
      e.writeUnionIndex(datum.getUnionIndex());
    }
    switch (datum.getType()) {
      case Type.NULL:
        e.writeNull();
        break;
      case Type.BOOLEAN:
        e.writeBoolean(datum.getValue!bool());
        break;
      case Type.INT:
        e.writeInt(datum.getValue!int());
        break;
      case Type.LONG:
        e.writeLong(datum.getValue!long());
        break;
      case Type.FLOAT:
        e.writeFloat(datum.getValue!float());
        break;
      case Type.DOUBLE:
        e.writeDouble(datum.getValue!double());
        break;
      case Type.STRING:
        e.writeString(datum.getValue!string());
        break;
      case Type.BYTES:
        e.writeBytes(datum.getValue!(ubyte[])());
        break;
      case Type.FIXED:
        e.writeFixed(datum.getValue!GenericFixed().getValue());
        break;
      case Type.RECORD:
        auto r = datum.getValue!GenericRecord();
        for (size_t i = 0; i < r.fieldCount(); i++) {
          write(r.fieldAt(i), e);
        }
        break;
      case Type.ENUM:
        e.writeEnum(datum.getValue!GenericEnum().getValue());
        break;
      case Type.ARRAY:
        GenericDatum[] items = datum.getValue!GenericArray().getValue();
        e.writeArrayStart();
        if (items.length > 0) {
          e.setItemCount(items.length);
          foreach (item; items) {
            e.startItem();
            write(item, e);
          }
        }
        e.writeArrayEnd();
        break;
      case Type.MAP:
        GenericDatum[string] items = datum.getValue!GenericMap().getValue();
        e.writeMapStart();
        if (items.length > 0) {
          e.setItemCount(items.length);
          foreach (key, item; items) {
            e.startItem();
            e.writeString(key);
            write(item, e);
          }
        }
        e.writeMapEnd();
        break;
      default:
        throw new AvroRuntimeException("Unknown schema type " ~ datum.getType().to!string);
    }
  }

  /// Constructs a writer for a given schema using the given encoder.
  this(Schema schema, Encoder encoder) {
    this.schema = schema;
    this.encoder = encoder;
  }

  /// Writes a value into the encoder.
  void write(GenericDatum datum) {
    write(datum, encoder);
  }
}

///
unittest {
  import std.array : appender;
  import avro.parser : Parser;
  import avro.codec.binaryencoder;

  auto parser = new Parser();
  Schema schema = parser.parseText(q"EOS
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
  GenericDatum datum = new GenericDatum(schema);
  assert(datum.getType == Type.RECORD);
  datum["name"].setValue("bob");
  assert(datum["name"].getValue!string == "bob");
  datum["favorite_number"].setUnionIndex(0);
  assert(datum["favorite_number"].getUnionIndex() == 0);
  datum["favorite_number"].setValue(8);
  assert(datum["favorite_number"].getValue!int == 8);
  datum["favorite_color"].setUnionIndex(0);
  assert(datum["favorite_color"].getUnionIndex() == 0);
  datum["favorite_color"].setValue("blue");
  assert(datum["favorite_color"].getValue!string == "blue");

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
}

unittest {
  import std.algorithm : map, joiner;
  import std.format : format;
  import std.array : appender;
  import avro.parser : Parser;
  import avro.codec.binaryencoder;

  auto parser = new Parser();
  Schema schema = parser.parseText(q"EOS
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "e", "type": {"type": "enum", "symbols": ["FULLTIME", "PARTTIME"], "name": "Job"}},
     {"name": "a", "type": {"type": "array", "items": "float"}},
     {"name": "m", "type": {"type": "map", "values": "long"}},
     {"name": "f", "type": {"type": "fixed", "size": 4, "name": "myfixed"}}
 ]
}
EOS");
  GenericDatum datum = new GenericDatum(schema);
  assert(datum.getType == Type.RECORD);
  datum["e"].getValue!(GenericEnum).setSymbol("PARTTIME");
  assert(datum["e"].getValue!(GenericEnum).getValue() == 1);
  pragma(msg, "typeof(datum[\"a\"]) = " ~ typeid(typeof(datum["a"])).stringof);
  datum["a"] ~= 1.23f;
  datum["a"] ~= 4.56f;
  assert(datum["a"].length == 2);
  datum["m"]["m1"] = 10L;
  datum["m"]["m2"] = 20L;
  assert(datum["m"]["m1"].getValue!long == 10L);
  datum["f"].getValue!(GenericFixed).setValue([0x01, 0x02, 0x03, 0x04]);
  assert(datum["f"].getValue!(GenericFixed).getValue() == [0x01, 0x02, 0x03, 0x04]);

  ubyte[] data;
  auto encoder = binaryEncoder(appender(&data));
  GenericWriter writer = new GenericWriter(schema, encoder);
  writer.write(datum);

  assert(data[0..1] == [
  // Field: e
  // idx=1
      0x02]);
  assert(data[1..11] == [
  // Field: a
  // len=2  1.23                    4.56                   len=0
      0x04, 0xa4, 0x70, 0x9d, 0x3f, 0x85, 0xeb, 0x91, 0x40, 0x00]);
  assert(data[11..12] == [
  // Field: m
  // len=2
      0x04]);
  assert(data[12..21] == [
  // len=2     m     2    20 len=2     m     1    10 len=0
      0x04, 0x6d, 0x32, 0x28, 0x04, 0x6D, 0x31, 0x14, 0x00]
      || data[12..21] == [
  // len=2     m     1    10 len=2     m     2    20 len=0
      0x04, 0x6d, 0x31, 0x14, 0x04, 0x6D, 0x32, 0x28, 0x00]);
  assert(data[21..25] == [
  // Field: f
  // 4-bytes
      0x01, 0x02, 0x03, 0x04]);
  //data.map!(a => format("0x%02x", a)).joiner(" ").to!string);
}
