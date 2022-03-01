/// Logic to encode Avro data types into binary format.
module avro.codec.binaryencoder;

import std.range : put, isOutputRange;
import std.conv : to;

import avro.codec.encoder : Encoder;
import avro.codec.zigzag : encodeInt, encodeLong;

/**
   An [Encoder] for Avro's binary encoding that does not buffer output.

   This encoder does not buffer writes on its own, and thus is best used with [BufferedOutputRange].
*/
class BinaryEncoder(ORangeT) : Encoder
if (isOutputRange!(ORangeT, ubyte))
{
  private ORangeT oRange;
  // the buffer is used for writing floats, doubles, and large longs.
  private ubyte[12] buf;

  /// Create a writer that sends its output to the underlying stream `oRange`.
  this(ORangeT oRange) {
    this.oRange = oRange;
  }

  override
  void writeNull() {
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      writeNull();
      writeNull();
    }
    assert(data == []);
  }


  override
  void writeBoolean(bool b) {
    put(oRange, b.to!ubyte);
  }

  override
  void writeInt(int n) {
    size_t len = encodeInt(n, buf[0..5]);
    put(oRange, buf[0..len]);
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      writeInt(-2);
      assert(data == [0x03]);
      writeInt(2147483647);
      assert(data == [0x03, 0xFE, 0xFF, 0xFF, 0xFF, 0x0F]);
    }
  }

  override
  void writeLong(long n) {
    size_t len = encodeLong(n, buf[0..10]);
    put(oRange, buf[0..len]);
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      writeLong(9223372036854775807);
      assert(data == [0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
      writeLong(-9223372036854775808);
      assert(data == [
              0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
              0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]);
    }
  }

  override
  void writeFloat(float f) {
    /// In DLang, floats use IEEE-754 format, matching Avro's format.
    /// See: https://dlang.org/spec/float.html
    const(ubyte)* p = cast(const(ubyte)*)(&f);
    put(oRange, p[0 .. float.sizeof]);
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      writeFloat(23.213121412);
      // Bytes according to IEEE-754 from least to most significant.
      assert(data == [0x79, 0xb4, 0xb9, 0x41]);
      writeFloat(-2423789437841.12112);
      assert(data == [
              0x79, 0xb4, 0xb9, 0x41,
              0x47, 0x15, 0x0d, 0xd4]);
    }
  }

  override
  void writeDouble(double d) {
    /// In DLang, floats use IEEE-754 format, matching Avro's format.
    /// See: https://dlang.org/spec/float.html
    const(ubyte)* p = cast(const(ubyte)*)(&d);
    put(oRange, p[0 .. double.sizeof]);
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      writeDouble(8329242423.24324);
      // Bytes according to IEEE-754 from least to most significant.
      assert(data == [0x50, 0xE4, 0x73, 0x73, 0x62, 0x07, 0xFF, 0x41]);
    }
  }

  override
  void writeString(string str) {
    import std.string : representation;
    if (str.length == 0) {
      writeZero();
      return;
    }
    ubyte[] bytes = str.representation.dup;
    super.writeBytes(bytes);
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      writeString("Grüßen");
      // Bytes according to IEEE-754 from least to most significant.
      //             len:8 zig-zag and var encoded
      assert(data == [0x10, 0x47, 0x72, 0xc3, 0xbc, 0xc3, 0x9f, 0x65, 0x6e]);
    }
  }

  // By default, superclass overloads are hidden.
  alias writeFixed = typeof(super).writeFixed;

  override
  void writeFixed(ubyte[] bytes, size_t start, size_t len)
  in (bytes.length >= start + len)
  {
    put(oRange, bytes[start .. (start + len)]);
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      ubyte[] bytes = [0x01, 0x02, 0x03, 0x04, 0x05];
      writeFixed(bytes, 3, 2);
      assert(data == [0x04, 0x05]);
      writeFixed(bytes);
      assert(data == [0x04, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05]);
    }
  }

  // By default, overloads of superclasses are hidden.
  alias writeBytes = typeof(super).writeBytes;

  override
  void writeBytes(ubyte[] bytes, size_t start, size_t len) {
    if (len == 0) {
      writeZero();
      return;
    }
    writeInt(len.to!int);
    writeFixed(bytes, start, len);
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      ubyte[] bytes = [0x01, 0x02, 0x03, 0x04, 0x05];
      writeBytes(bytes, 3, 2);
      // len=0x02 => zig-zag + var encoding => 0x04
      assert(data == [0x04, 0x04, 0x05]);
      writeBytes(bytes);
      // len=0x05 => zig-zag + var encoding => 0x0A
      assert(data == [0x04, 0x04, 0x05, 0x0A, 0x01, 0x02, 0x03, 0x04, 0x05]);
    }
  }

  override
  void writeEnum(int e) {
    writeInt(e);
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      writeEnum(0);
      assert(data == [0x00]);
      writeEnum(3);
      assert(data == [0x00, 0x06]);
    }
  }


  override
  void writeArrayStart() {
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      writeArrayStart();
      setItemCount(2);
      startItem();
      writeLong(5L);
      writeBoolean(true);
      startItem();
      writeLong(-8L);
      writeBoolean(false);
      writeArrayEnd();
    }
    //              len-1 item1       item2       len-2
    assert(data == [0x04, 0x0A, 0x01, 0x0F, 0x00, 0x00]);
  }

  override
  void setItemCount(size_t itemCount) {
    if (itemCount > 0) {
      writeLong(itemCount.to!long);
    }
  }

  override
  void startItem() {
  }

  override
  void writeArrayEnd() {
    writeZero();
  }

  override
  void writeMapStart() {
  }

  ///
  unittest {
    import std.array : appender;
    ubyte[] data;
    auto encoder = binaryEncoder(appender(&data));
    with (encoder) {
      // A map of records, each of which has a Long and a Boolean.
      writeMapStart();
      setItemCount(2);
      // Item 1
      startItem();
      writeString("ham");
      writeLong(3);
      writeBoolean(true);
      // Item 2
      startItem();
      writeString("cat");
      writeLong(-3);
      writeBoolean(false);
      // End
      writeMapEnd();
    }
    assert(data == [
         // blk1
            0x04,
         // sLen     h     a     m  long  bool
            0x06, 0x68, 0x61, 0x6d, 0x06, 0x01,
         // sLen     c     a     t  long  bool
            0x06, 0x63, 0x61, 0x74, 0x05, 0x00,
         // blk2
            0x00]);
  }

  override
  void writeMapEnd() {
    writeZero();
  }

  override
  void writeUnionIndex(int unionIndex) {
    writeInt(unionIndex);
  }

  override
  void flush() {
    static if (is(typeof(ORangeT.init.flush()))) {
      oRange.flush();
    }
  }

  void writeZero() {
    put(oRange, 0.to!ubyte);
  }

}

/// A helper function for constructing a [BinaryEncoder] with inferred template arguments.
auto binaryEncoder(ORangeT)(ORangeT oRange) {
  return new BinaryEncoder!(ORangeT)(oRange);
}

/// writeBoolean
unittest {
  import std.array : appender;
  import avro.codec.bufferedoutputrange : bufferedOutputRange;

  // Here we write into an array, but a file or other range can work.
  ubyte[] data;

  // Demonstrating the use of a buffered ouput range with the binary encoder.
  auto encoder = binaryEncoder(bufferedOutputRange!ubyte(appender(&data)));

  with (encoder) {
    writeBoolean(true);
    assert(data == []);
    flush();
    assert(data == [1]);
    writeBoolean(false);
    flush();
    assert(data == [1, 0]);
  }
}

// writeNull
unittest {
  import std.array : appender;
  ubyte[] data;
  auto encoder = binaryEncoder(appender(&data));
  with (encoder) {
    writeNull();
    writeNull();
  }
  assert(data == []);
}

