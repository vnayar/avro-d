/// Logic to deserialize Avro data encoded in binary format.
module avro.codec.binarydecoder;

import std.conv : to, ConvOverflowException;
import std.range;
import std.string : assumeUTF;
import std.exception : assertThrown;
import avro.codec.zigzag : decodeZigzagLong;
import avro.codec.decoder : Decoder;
import avro.exception : AvroRuntimeException, InvalidNumberEncodingException;

/**
   A [Decoder] for binary-format data.

   This class may read-ahead and buffer bytes from the source beyond what is
   required to serve its read methods. The number of unused bytes in the buffer
   can be accessed by inputStream().remaining(), if the BinaryDecoder is not
   'direct'.

   See_Also: [Encoder]
*/
class BinaryDecoder(IRangeT) : Decoder
if (isInputRange!IRangeT && is(ElementType!(IRangeT) : ubyte))
{
  private IRangeT iRange;

  this(IRangeT iRange) {
    this.iRange = iRange;
  }

  override
  void readNull() {
  }

  ///
  unittest {
    ubyte[] data = [0x00, 0x01];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      readNull();
      readNull();
    }
    // No exceptions or other problems.
  }

  override
  bool readBoolean() {
    bool val = iRange.front.to!bool;
    doSkipBytes(bool.sizeof);
    return val;
  }

  ///
  unittest {
    ubyte[] data = [0x00, 0x01, 0x02];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      assert(readBoolean() == false);
      assert(readBoolean() == true);
      assertThrown!ConvOverflowException(readBoolean());
    }
    // No exceptions or other problems.
  }

  override
  int readInt() {
    long val = doReadLong();
    return val.to!int;
  }

  ///
  unittest {
    ubyte[] data = [0x03, 0xFE, 0xFF, 0xFF, 0xFF, 0x0F];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      assert(readInt() == -2);
      assert(readInt() == 2147483647);
    }
  }

  override
  long readLong() {
    long val = doReadLong();
    return val;
  }

  ///
  unittest {
    ubyte[] data = [
        0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      assert(readLong() == 9223372036854775807);
      assert(readLong() == -9223372036854775808);
    }
  }

  override
  float readFloat() {
    ubyte[] bytes = iRange.take(float.sizeof)[];
    doSkipBytes(float.sizeof);
    return *(cast(float*)(bytes.ptr));
  }

  ///
  unittest {
    ubyte[] data = [
        0x6e, 0xb4, 0xb9, 0x41,
        0x34, 0xa0, 0x99, 0xc2];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      assert(readFloat() == 23.2131f);
      assert(readFloat() == -76.8129f);
    }
  }

  override
  double readDouble() {
    ubyte[] bytes = iRange.take(double.sizeof)[];
    doSkipBytes(double.sizeof);
    return *(cast(double*)(bytes.ptr));
  }

  ///
  unittest {
    ubyte[] data = [
        0x50, 0xE4, 0x73, 0x73, 0x62, 0x07, 0xFF, 0x41];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      double val;
      assert((val = readDouble()) == 8329242423.24324, val.to!string);
    }
  }

  override
  string readString() {
    size_t len = doReadLength();
    ubyte[] bytes = iRange.take(len)[];
    doSkipBytes(len);
    return bytes.assumeUTF;
  }

  ///
  unittest {
    ubyte[] data = [
        0x10, 0x47, 0x72, 0xc3, 0xbc, 0xc3, 0x9f, 0x65, 0x6e];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      string val;
      assert((val = readString()) == "Grüßen", val.to!string);
    }
  }

  override
  void skipString() {
    size_t len = doReadLength();
    doSkipBytes(len);
  }

  ///
  unittest {
    ubyte[] data = [
        0x0C, 0x56, 0x69, 0x65, 0x6C, 0x65, 0x6E,              // "Vielen"
        0x10, 0x47, 0x72, 0xc3, 0xbc, 0xc3, 0x9f, 0x65, 0x6e]; // "Grüßen"
    auto decoder = binaryDecoder(data);
    with (decoder) {
      skipString();  // Detect and skip over the first string.
      string val;
      assert((val = readString()) == "Grüßen", val.to!string);
    }
  }

  override
  ubyte[] readBytes() {
    size_t len = doReadLength();
    ubyte[] bytes = iRange.take(len)[];
    doSkipBytes(len);
    return bytes;
  }

  ///
  unittest {
    ubyte[] data = [0x0C, 0x56, 0x69, 0x65, 0x6C, 0x65, 0x6E];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      ubyte[] bytes = readBytes();
      assert(bytes == [0x56, 0x69, 0x65, 0x6C, 0x65, 0x6E]);
    }
  }

  override
  void skipBytes() {
    size_t len = doReadLength();
    doSkipBytes(len);
  }

  ///
  unittest {
    ubyte[] data = [
        0x0C, 0x56, 0x69, 0x65, 0x6C, 0x65, 0x6E,
        0x02, 0x03];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      skipBytes();
      ubyte[] bytes = readBytes();
      assert(bytes == [0x03]);
    }
  }

  override
  ubyte[] readFixed(size_t length) {
    ubyte[] bytes = iRange.take(length)[];
    iRange.popFrontN(length);
    return bytes;
  }

  ///
  unittest {
    ubyte[] data = [0x0C, 0x56, 0x69, 0x65, 0x6C, 0x65, 0x6E];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      ubyte[] bytes = readFixed(3);
      assert(bytes == [0x0C, 0x56, 0x69]);
    }
  }

  override
  void skipFixed(size_t length) {
    doSkipBytes(length);
  }

  ///
  unittest {
    ubyte[] data = [0x0C, 0x56, 0x69, 0x65, 0x6C, 0x65, 0x6E];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      skipFixed(3);
      ubyte[] bytes = readFixed(3);
      assert(bytes == [0x65, 0x6C, 0x65]);
    }
  }

  /// See_Also: [readInt]
  override
  int readEnum() {
    return readInt();
  }

  private void doSkipBytes(size_t length) {
    iRange.popFrontN(length);
  }

  /**
     Returns the number of items to follow in the current array or map. Returns 0
     if there are no more items in the current array and the array/map has ended.
     Arrays are encoded as a series of blocks. Each block consists of a long count
     value, followed by that many array items. A block with count zero indicates
     the end of the array. If a block's count is negative, its absolute value is
     used, and the count is followed immediately by a long block size indicating
     the number of bytes in the block.

     Throws: IOException If the first byte cannot be read for any reason other
     than the end of the file, if the input stream has been
     closed, or if some other I/O error occurs.
  */
  private size_t doReadItemCount() {
    long result = readLong();
    if (result < 0L) {
      // Consume byte-count if present
      readLong();
      result = -result;
    }
    return result.to!size_t;
  }

  /**
     Reads the count of items in the current array or map and skip those items, if
     possible. If it could skip the items, keep repeating until there are no more
     items left in the array or map. Arrays are encoded as a series of blocks.
     Each block consists of a long count value, followed by that many array items.
     A block with count zero indicates the end of the array. If a block's count is
     negative, its absolute value is used, and the count is followed immediately
     by a long block size indicating the number of bytes in the block. If block
     size is missing, this method return the count of the items found. The client
     needs to skip the items individually.

     Returns: Zero if there are no more items to skip and end of array/map is reached. Positive
     number if some items are found that cannot be skipped and the client needs to skip them
     individually.

     Throws: IOException If the first byte cannot be read for any reason other
     than the end of the file, if the input stream has been
     closed, or if some other I/O error occurs.
  */
  private size_t doSkipItems() {
    long result = readLong();
    while (result < 0L) {
      long bytecount = readLong();
      doSkipBytes(bytecount);
      result = readLong();
    }
    return result.to!size_t;
  }

  override
  size_t readArrayStart() {
    return doReadItemCount();
  }

  ///
  unittest {
    //                 4    -3    16
    ubyte[] data = [0x08, 0x05, 0x20, 0x65];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      assert(readArrayStart() == 4);
      assert(readArrayStart() == 3);
      assert(readFixed(1) == [0x65]);
    }
  }

  /// See_Also: readArrayStart
  override
  size_t arrayNext() {
    return doReadItemCount();
  }

  override
  long skipArray() {
    return doSkipItems();
  }

  ///
  unittest {
    // A binary array with 2 items in a 6-byte block, and 1 block only.
    ubyte[] data = [
    //    -2     6                                   empty block
        0x05, 0x0C, 0x20, 0x21, 0x22, 0x30, 0x31, 0x32, 0x00,
    // Another array, but without an indicator of bytes per block.
        0x04, 0x20, 0x21, 0x22, 0x30, 0x31, 0x32, 0x00];
    auto decoder = binaryDecoder(data);
    with (decoder) {
      assert(skipArray() == 0);
      assert(skipArray() == 2);
    }
  }

  /// See_Also: readArrayStart
  override
  size_t readMapStart() {
    return doReadItemCount();
  }

  /// See_Also: readArrayStart
  override
  size_t mapNext() {
    return doReadItemCount();
  }

  /// See_Also: [skipArray]
  override
  size_t skipMap() {
    return doSkipItems();
  }

  /// See_Also: [readInt]
  override
  int readUnionIndex() {
    return readInt();
  }

  private size_t doReadLength() {
    long len = readInt();
    if (len < 0) {
      throw new AvroRuntimeException("Cannot have negative length: " ~ len.to!string);
    }
    return cast(size_t) len;
  }

  /// Reads bytes from the input stream to decode a variable-length zigzag integer.
  private long doReadLong() {
    ulong encoded = 0;
    int shift = 0;
    ubyte u;
    do {
      if (shift >= 64) {
        throw new AvroRuntimeException("Invalid Avro varint");
      }
      u = iRange.front;
      iRange.popFront;
      encoded |= cast(ulong)(u & 0x7f) << shift;
      shift += 7;
    } while (u & 0x80);
    return decodeZigzagLong(encoded);
  }
}

auto binaryDecoder(IRangeT)(IRangeT iRange) {
  return new BinaryDecoder!IRangeT(iRange);
}

unittest {
  ubyte[] data = [0x01, 0x00];
  auto decoder = binaryDecoder(data);
  with (decoder) {
    assert(readBoolean() == true);
  }
}
