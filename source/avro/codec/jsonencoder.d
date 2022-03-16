/// Encodes an Avro object as JSON.
module avro.codec.jsonencoder;

import std.algorithm : map;
import std.range : put, isOutputRange;
import std.conv : to;

import avro.codec.encoder : Encoder;
import avro.codec.jsonutil : encodeJsonString;

/// An [Encoder] for Avro's JSON encoding that does not buffer output.
class JsonEncoder(ORangeT) : Encoder
if (isOutputRange!(ORangeT, char))
{
  private ORangeT oRange;
  private bool firstItem = true;

  this (ORangeT oRange) {
    this.oRange = oRange;
  }

  override
  void writeNull() {
    put(oRange, "null");
  }

  override
  void writeBoolean(bool b) {
    put(oRange, b ? "true" : "false");
  }

  override
  void writeInt(int n) {
    put(oRange, n.to!string);
  }

  override
  void writeLong(long n) {
    put(oRange, n.to!string);
  }

  override
  void writeFloat(float f) {
    import std.math : isNaN;
    if (f == float.infinity) {
      put(oRange, "Infinity");
    } else if (f == -float.infinity) {
      put(oRange, "-Infinity");
    } else if (isNaN(f)) {
      put(oRange, "NaN");
    } else {
      put(oRange, f.to!string);
    }
  }

  override
  void writeDouble(double d) {
    import std.math : isNaN;
    if (d == double.infinity) {
      put(oRange, "Infinity");
    } else if (d == -double.infinity) {
      put(oRange, "-Infinity");
    } else if (isNaN(d)) {
      put(oRange, "NaN");
    } else {
      put(oRange, d.to!string);
    }
  }

  override
  void writeString(string str) {
    put(oRange, "\"");
    put(oRange, encodeJsonString(str));
    put(oRange, "\"");
  }

  override
  void writeRecordKey(string key) {
    writeMapKey(key);
  }

  override
  void writeMapKey(string key) {
    writeString(key);
    put(oRange, ": ");
  }

  // By default, superclass overloads are hidden.
  alias writeFixed = typeof(super).writeFixed;

  override
  void writeFixed(ubyte[] bytes, size_t start, size_t len)
  in (bytes.length >= start + len)
  {
    writeString(bytes.map!(to!char).to!string);
  }

  // By default, overloads of superclasses are hidden.
  alias writeBytes = typeof(super).writeBytes;

  override
  void writeBytes(ubyte[] bytes, size_t start, size_t len) {
    writeFixed(bytes, start, len);
  }


  override
  void writeEnum(size_t e, string sym) {
    writeString(sym);
  }

  override
  void writeArrayStart() {
    put(oRange, "[");
    firstItem = true;
  }

  override
  void setItemCount(size_t itemCount) {
  }

  override
  void startItem() {
    if (!firstItem)
      put(oRange, ", ");
    firstItem = false;
  }

  override
  void writeArrayEnd() {
    put(oRange, "]");
  }

  override
  void writeMapStart() {
    put(oRange, "{");
    firstItem = true;
  }

  override
  void writeMapEnd() {
    put(oRange, "}");
  }

  override
  void writeRecordStart() {
    writeMapStart();
  }

  override
  void writeRecordEnd() {
    writeMapEnd();
  }

  override
  void writeUnionStart() {
    put(oRange, "{");
  }

  override
  void writeUnionType(size_t unionTypeIndex, string unionTypeName) {
    writeMapKey(unionTypeName);
  }

  override
  void writeUnionEnd() {
    writeMapEnd();
  }

  override
  void flush() {
    static if (is(typeof(ORangeT.init.flush()))) {
      oRange.flush();
    }
  }
}

/// A helper function for constructing a [BinaryEncoder] with inferred template arguments.
auto jsonEncoder(ORangeT)(ORangeT oRange) {
  return new JsonEncoder!(ORangeT)(oRange);
}

///
unittest {
  import std.stdio;
  import std.format;
  import std.array : appender;

  struct Test {
    void function(Encoder) testOp;
    string expected;
  }
  auto tests = [
      Test((e) => e.writeNull(), "null"),
      Test((e) => e.writeBoolean(true), "true"),
      Test((e) => e.writeBoolean(false), "false"),
      Test((e) => e.writeInt(-101), "-101"),
      Test((e) => e.writeLong(9223372036854775807L), "9223372036854775807"),
      Test((e) => e.writeFloat(0.002358), "0.002358"),
      Test((e) => e.writeFloat(float.nan), "NaN"),
      Test((e) => e.writeFloat(float.infinity), "Infinity"),
      Test((e) => e.writeFloat(-float.infinity), "-Infinity"),
      Test((e) => e.writeDouble(0.002358), "0.002358"),
      Test((e) => e.writeDouble(double.nan), "NaN"),
      Test((e) => e.writeDouble(double.infinity), "Infinity"),
      Test((e) => e.writeDouble(-double.infinity), "-Infinity"),
      Test((e) => e.writeString("ham"), "\"ham\""),
      Test((e) => e.writeString("a\nb\tc\0"), "\"a\\nb\\tc\\u0000\""),
      Test((e) => e.writeFixed([0x00, 0x01, 0x41, 0x42, 0x7E, 0x22]), "\"\\u0000\\u0001AB~\\\"\""),
      Test((e) => e.writeBytes([0x00, 0x01, 0x41, 0x42, 0x7E, 0x22]), "\"\\u0000\\u0001AB~\\\"\""),
      Test((e) => e.writeEnum(3, "CAT"), "\"CAT\""),
      Test((e) {
            e.writeArrayStart();
            e.setItemCount(2);
            e.startItem();
            e.writeInt(3);
            e.startItem();
            e.writeInt(4);
            e.writeArrayEnd();
          }, "[3, 4]"),
      Test((e) {
            e.writeMapStart();
            e.setItemCount(2);
            e.startItem();
            e.writeMapKey("a");
            e.writeInt(3);
            e.startItem();
            e.writeMapKey("b");
            e.writeInt(4);
            e.writeMapEnd();
          }, `{"a": 3, "b": 4}`),
      Test((e) {
            e.writeRecordStart();
            e.setItemCount(2);
            e.startItem();
            e.writeRecordKey("a");
            e.writeInt(3);
            e.startItem();
            e.writeRecordKey("b");
            e.writeInt(4);
            e.writeRecordEnd();
          }, `{"a": 3, "b": 4}`),
      Test((e) {
            e.writeUnionStart();
            e.writeUnionType(1, "CAT");
            e.writeInt(3);
            e.writeUnionEnd();
          }, `{"CAT": 3}`),
  ];

  foreach (test; tests) {
    string data;
    auto encoder = jsonEncoder(appender(&data));
    test.testOp(encoder);
    string val;
    assert(
        (val = data.map!(to!char).to!string) == test.expected,
        format("Expected '%s' got '%s'.", test.expected, val));
  }
}
