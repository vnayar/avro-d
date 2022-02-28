/**
   Methods for encoding and decoding integer values using zig-zag and variable-length coding.

   Variable-length coding is far more effective with negative numbers when used on zig-zag
   encoded values.

   See_Also:
   - [zig-zag coding](https://developers.google.com/protocol-buffers/docs/encoding?csw=1#types)
   - [variable-length coding](https://lucene.apache.org/core/3_5_0/fileformats.html#VInt)
*/
module avro.codec.zigzag;

/// Perform zig-zag encoding for a 64-bit long.
ulong encodeZigzagLong(long input) nothrow pure {
  return ((input << 1) ^ (input >> 63));
}

unittest {
  assert(encodeZigzagLong( 0) == 0);
  assert(encodeZigzagLong(-1) == 1);
  assert(encodeZigzagLong( 1) == 2);
  assert(encodeZigzagLong(-2) == 3);
  assert(encodeZigzagLong( 2147483647) == 4294967294);
  assert(encodeZigzagLong(-2147483648) == 4294967295);

  assert(encodeZigzagLong(9223372036854775807) == 18446744073709551614);
  assert(encodeZigzagLong(-9223372036854775808) == 18446744073709551615);
}

/// Perform zig-zag decoding for a 64-bit long.
long decodeZigzagLong(ulong input) nothrow pure {
  return cast(long)(((input >> 1) ^ -(cast(long)(input) & 1)));
}

unittest {
  assert(decodeZigzagLong(0) == 0);
  assert(decodeZigzagLong(1) == -1);
  assert(decodeZigzagLong(2) == 1);
  assert(decodeZigzagLong(3) == -2);
  assert(decodeZigzagLong(4294967294) == 2147483647);
  assert(decodeZigzagLong(4294967295) == -2147483648);

  assert(decodeZigzagLong(18446744073709551614) == 9223372036854775807);
  assert(decodeZigzagLong(18446744073709551615) == -9223372036854775808);
}

/// Perform zig-zag encoding for a 32-bit int.
uint encodeZigzagInt(int input) nothrow pure {
  return ((input << 1) ^ (input >> 31));
}

unittest {
  assert(encodeZigzagInt(0) == 0);
  assert(encodeZigzagInt(-1) == 1);
  assert(encodeZigzagInt(1) == 2);
  assert(encodeZigzagInt(-2) == 3);
  assert(encodeZigzagInt(2147483647) == 4294967294);
  assert(encodeZigzagInt(-2147483648) == 4294967295);
}

/// Perform zig-zag decoding for a 32-bit int.
int decodeZigzagInt(uint input) nothrow pure {
  return cast(int)(((input >> 1) ^ -(cast(int)(input) & 1)));
}

unittest {
  assert(decodeZigzagInt(0) == 0);
  assert(decodeZigzagInt(1) == -1);
  assert(decodeZigzagInt(2) == 1);
  assert(decodeZigzagInt(3) == -2);
  assert(decodeZigzagInt(4294967294) == 2147483647);
  assert(decodeZigzagInt(4294967295) == -2147483648);
}

/// Encodes a long into a variable number of bytes in a given buffer.
size_t encodeLong(long input, ref ubyte[10] output) nothrow {
  ulong val = encodeZigzagLong(input);

  // put values in an array of bytes with variable length encoding.
  enum int mask = 0x7F;
  ubyte v = val & mask;
  size_t bytesOut = 0;
  while (val >>= 7) {
    output[bytesOut++] = (v | 0x80);
    v = val & mask;
  }

  output[bytesOut++] = v;
  return bytesOut;
}

unittest {
  ubyte[12] bytes;
  size_t n;
  n = encodeLong( 0, bytes[0..10]);
  assert(bytes[0..n] == [0b00000000]);

  n = encodeLong( 1, bytes[0..10]);
  assert(bytes[0..n] == [0b00000010]);

  n = encodeLong(-2, bytes[0..10]);
  assert(bytes[0..n] == [0b00000011]);

  n = encodeLong( 2147483647, bytes[0..10]);
  assert(bytes[0..n] == [0b11111110, 0b11111111, 0b11111111, 0b11111111, 0b00001111]);

  n = encodeLong(-2147483648, bytes[0..10]);
  assert(bytes[0..n] == [0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b00001111]);

  n = encodeLong(9223372036854775807, bytes[0..10]);
  assert(bytes[0..n] == [
          0b11111110, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111,
          0b11111111, 0b11111111, 0b11111111, 0b00000001]);

  n = encodeLong(-9223372036854775808, bytes[0..10]);
  assert(bytes[0..n] == [
          0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111,
          0b11111111, 0b11111111, 0b11111111, 0b00000001]);
}

/// Encodes an int into a variable number of bytes in a given buffer.
size_t encodeInt(int input, ref ubyte[5] output) /*nothrow*/ {
  uint val = encodeZigzagInt(input);

  // put values in an array of bytes with variable length encoding
  enum int mask = 0x7F;
  ubyte v = val & mask;
  size_t bytesOut = 0;
  while (val >>= 7) {
    output[bytesOut++] = (v | 0x80);
    v = val & mask;
  }
  output[bytesOut++] = v;
  return bytesOut;
}

unittest {
  ubyte[12] bytes;
  size_t n;
  n = encodeInt( 0, bytes[0..5]);
  assert(bytes[0..n] == [0b00000000]);

  n = encodeInt( 1, bytes[0..5]);
  assert(bytes[0..n] == [0b00000010]);

  n = encodeInt(-2, bytes[0..5]);
  assert(bytes[0..n] == [0b00000011]);

  n = encodeInt( 2147483647, bytes[0..5]);
  assert(bytes[0..n] == [0b11111110, 0b11111111, 0b11111111, 0b11111111, 0b00001111]);

  n = encodeInt(-2147483648, bytes[0..5]);
  assert(bytes[0..n] == [0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b00001111]);
}
