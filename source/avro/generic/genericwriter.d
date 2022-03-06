module avro.generic.genericwriter;

import std.conv : to;
import avro.generic.genericdata :
    GenericDatum, GenericFixed, GenericRecord, GenericEnum, GenericArray, GenericUnion, GenericMap;
import avro.exception : AvroRuntimeException, AvroTypeException;
import avro.type : Type;
import avro.schema : Schema;
import avro.codec.datumwriter : DatumWriter;
import avro.codec.encoder : Encoder;

/// [DatumWriter] for generic Java objects.
class GenericWriter {
  private Schema schema;
  private Encoder encoder;

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
  import std.stdio;
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
