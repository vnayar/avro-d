module avro.generic.genericreader;

import std.conv : to;

import avro.codec.decoder : Decoder;
import avro.generic.genericdata
    : GenericDatum, GenericFixed, GenericRecord, GenericArray, GenericEnum, GenericMap;
import avro.schema : Schema;
import avro.type : Type;

/// A utility class to read GenericDatum from decoders.
class GenericReader {
  private Schema schema;
  private Decoder decoder;
  private bool isResolving;

  /// Uses a given decoder to read [GenericDatum] from its serialized format.
  static void read(Decoder d, bool isResolving, GenericDatum datum) {
    if (datum.isUnion()) {
      datum.setUnionIndex(d.readUnionIndex(datum.getUnionSchema()));
    }
    switch (datum.getType()) {
      case Type.NULL:
        d.readNull();
        break;
      case Type.BOOLEAN:
        datum.setValue(d.readBoolean());
        break;
      case Type.INT:
        datum.setValue(d.readInt());
        break;
      case Type.LONG:
        datum.setValue(d.readLong());
        break;
      case Type.FLOAT:
        datum.setValue(d.readFloat());
        break;
      case Type.DOUBLE:
        datum.setValue(d.readDouble());
        break;
      case Type.STRING:
        datum.setValue(d.readString());
        break;
      case Type.BYTES:
        datum.setValue(d.readBytes());
        break;
      case Type.FIXED:
        auto f = datum.getValue!GenericFixed();
        datum.setValue(d.readFixed(f.getSchema().getFixedSize()));
        break;
      case Type.RECORD:
        auto r = datum.getValue!GenericRecord();
        if (isResolving) {
          // TODO
          throw new Exception("Not implemented!");
        } else {
          d.readRecordStart();
          for (size_t i = 0; i < r.fieldCount(); i++) {
            d.readRecordKey();
            read(d, isResolving, r.fieldAt(i));
          }
          d.readRecordEnd();
        }
        break;
      case Type.ENUM:
        auto enumDatum = datum.getValue!GenericEnum();
        enumDatum.setEnumOrdinal(d.readEnum(enumDatum.getSchema()));
        break;
      case Type.ARRAY:
        auto v = datum.getValue!GenericArray();
        GenericDatum[] arr = v.getValue();
        const(Schema) elemSchema = v.getSchema().getElementSchema();
        arr.length = 0;
        size_t start = 0;
        for (size_t m = d.readArrayStart(); m != 0; m = d.readArrayNext()) {
          arr.length += m;
          for (; start < arr.length; ++start) {
            arr[start] = new GenericDatum(elemSchema);
            read(d, isResolving, arr[start]);
          }
        }
        break;
      case Type.MAP:
        auto v = datum.getValue!GenericMap();
        GenericDatum[string] r = v.getValue();
        const(Schema) valueSchema = v.getSchema().getValueSchema();
        r.clear();
        size_t start = 0;
        for (size_t m = d.readMapStart(); m != 0; m = d.readMapNext()) {
          for (size_t j = 0; j < m; j++) {
            string key = d.readString();
            GenericDatum value = new GenericDatum(valueSchema);
            read(d, isResolving, value);
            r[key] = value;
          }
        }
        r.rehash();
        break;
      default:
        throw new Exception("Unknown schema type: " ~ datum.getType().to!string);
    }
    if (datum.isUnion() && datum.getType() != Type.NULL) {
      d.readUnionEnd();
    }
  }

  /// Constructs a reader for the given schema using the given decoder.
  this(Schema s, Decoder decoder) {
    this.schema = s;
    //this.isResolving = decoder !is null;
    this.isResolving = false;
    this.decoder = decoder;
  }

  // /**
  //    Constructs a reader for the given reader's schema using the given decoder which holds data
  //    matching the writer's schema.

  //    TODO: Implement a resolving decoder to validate two schemas.
  // */
  // this(Schema writerSchema, Schema readerSchema, Decoder decoder) {
  //   this.schema = readerSchema;
  //   this.isResolving = true;
  //   this.decoder = resolvingDecoder(writerSchema, readerSchema, decoder);
  // }

  /// Reads a value off the decoder.
  void read(ref GenericDatum datum) {
    datum = new GenericDatum(schema);
    read(decoder, isResolving, datum);
  }
}

///
unittest {
  import avro.parser : Parser;
  import avro.codec.binarydecoder;

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

  ubyte[] data = [
  // Field: name
  // len=3     b     o     b
      0x06, 0x62, 0x6F, 0x62,
  // Field: favorite_number
  // idx=0     8
      0x00, 0x10,
  // Field: favorite_color
  // idx=0 len=4     b     l     u     e
      0x00, 0x08, 0x62, 0x6C, 0x75, 0x65
  ];
  auto decoder = binaryDecoder(data);
  GenericReader reader = new GenericReader(schema, decoder);
  GenericDatum datum;
  reader.read(datum);

  assert(datum["name"].getValue!string() == "bob");
  assert(datum["favorite_number"].getValue!int() == 8);
  assert(datum["favorite_color"].getValue!string() == "blue");
}

///
unittest {
  import std.stdio;
  import avro.parser : Parser;
  import avro.codec.jsondecoder;

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

  string data = q"EOS
{
  "name": "bob",
  "favorite_number": {"int": 8},
  "favorite_color": null
}
EOS";
  auto decoder = jsonDecoder(data);
  GenericReader reader = new GenericReader(schema, decoder);
  GenericDatum datum;
  reader.read(datum);

  assert(datum["name"].getValue!string() == "bob");
  assert(datum["favorite_number"].getValue!int() == 8);
  assert(datum["favorite_color"].getType() == Type.NULL);
}
