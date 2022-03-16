/// Support logic for deserializing Avro values.
module avro.codec.decoder;

import avro.schema : Schema;

/**
  Low-level support for de-serializing Avro values.

  This class has two types of methods. One type of methods support the reading
  of leaf values (for example, [Decoder.readLong] and [Decoder.readString]).

  The other type of methods support the reading of maps and arrays. These
  methods are [Decoder.readArrayStart], [Decoder.arrayNext], and similar methods
  for maps. See [Decoder.readArrayStart] for details on these methods.
*/
abstract class Decoder {

  /**
     "Reads" a null value. (Doesn't actually read anything, but advances the state
     of the parser if the implementation is stateful.)

     Throws: [AvroTypeException] If this is a stateful reader and null is not the
     type of the next value to be read
  */
  abstract void readNull();

  /**
    Reads a boolean value written by [Encoder.writeBoolean].

    Throws: [AvroTypeException] If this is a stateful reader and boolean is not the type of the next
    value to be read
   */

  abstract bool readBoolean();

  /**
     Reads an integer written by [Encoder.writeInt].

     Throws:
     - AvroTypeException If encoded value is larger than 32-bits.
     - AvroTypeException If this is a stateful reader and int is not the type of
       the next value to be read
  */
  abstract int readInt();

  /**
     Reads a long written by [Encoder.writeLong].

     Throws: AvroTypeException If this is a stateful reader and long is not the type of the next
     value to be read
   */
  abstract long readLong();

  /**
    Reads a float written by [Encoder.writeFloat].

    Throws: AvroTypeException If this is a stateful reader and is not the type of the next value to
    be read.
   */
  abstract float readFloat();

  /**
     Reads a double written by [Encoder.writeDouble].

     Throws: AvroTypeException If this is a stateful reader and is not the type of the next value to
     be read.
   */
  abstract double readDouble();

  /**
     Reads a char-string written by [Encoder.writeString].

     Throws: AvroTypeException If this is a stateful reader and char-string is not
     the type of the next value to be read.
  */
  abstract string readString();

  /**
     Discards a char-string written by [Encoder.writeString].

     Throws: AvroTypeException If this is a stateful reader and char-string is not
     the type of the next value to be read
  */
  abstract void skipString();

  /**
     Reads a byte-string written by [Encoder.writeBytes]. if `old` is
     not null and has sufficient capacity to take in the bytes being read, the
     bytes are returned in `old`.

     Throws: AvroTypeException If this is a stateful reader and byte-string is not
     the type of the next value to be read
  */
  abstract ubyte[] readBytes();

  /**
     Discards a byte-string written by [Encoder.writeBytes].

     Throws: AvroTypeException If this is a stateful reader and byte-string is not
     the type of the next value to be read
  */
  abstract void skipBytes();

  /**
     Reads fixed sized binary object.

     Params:
       length = The size of the binary object.
     Throws:
     - AvroTypeException If this is a stateful reader and fixed sized binary
       object is not the type of the next value to be read
       or the length is incorrect.
     - IOException
     Returns: The fixed size binary object.
  */
  abstract ubyte[] readFixed(size_t length);

  /**
     Discards fixed sized binary object.

     Params:
       length = The size of the binary object to be skipped.

     Throws:
     - AvroTypeException If this is a stateful reader and fixed sized binary
       object is not the type of the next value to be read
       or the length is incorrect.
     - IOException
  */
  abstract void skipFixed(size_t length);

  /**
     Reads an enumeration.

     Returns: The enumeration's value.
     Throws:
     - AvroTypeException If this is a stateful reader and enumeration is not
       the type of the next value to be read.
     - IOException
  */
  abstract size_t readEnum(const Schema enumSchema);

  abstract void readRecordStart();

  abstract void readRecordKey();

  abstract void readRecordEnd();

  /**
     Reads and returns the size of the first block of an array. If this method
     returns non-zero, then the caller should read the indicated number of items,
     and then call [arrayNext] to find out the number of items in the next
     block. The typical pattern for consuming an array looks like:

     ---
     for(long i = in.readArrayStart(); i != 0; i = in.arrayNext()) {
       for (long j = 0; j < i; j++) {
         read next element of the array;
       }
     }
     ---

     Throws: AvroTypeException If this is a stateful reader and array is not the
     type of the next value to be read
  */
  abstract size_t readArrayStart();

  /**
     Processes the next block of an array and returns the number of items in the
     block and let's the caller read those items.

     Throws: AvroTypeException When called outside of an array context
  */
  abstract size_t readArrayNext();

  /**
     Used for quickly skipping through an array. Note you can either skip the
     entire array, or read the entire array (with [readArrayStart]), but
     you can't mix the two on the same array.

     This method will skip through as many items as it can, all of them if
     possible. It will return zero if there are no more items to skip through, or
     an item count if it needs the client's help in skipping. The typical usage
     pattern is:

     ---
     for (long i = in.skipArray(); i != 0; i = i.skipArray()) {
       for (long j = 0; j < i; j++) {
         read and discard the next element of the array;
       }
     }
     ---

     Note that this method can automatically skip through items if a byte-count is
     found in the underlying data, or if a schema has been provided to the
     implementation, but otherwise the client will have to skip through items
     itself.

     Throws: AvroTypeException If this is a stateful reader and array is not the
     type of the next value to be read
  */
  abstract long skipArray();

  /**
    Reads and returns the size of the next block of map-entries. Similar to
    [readArrayStart].

    As an example, let's say you want to read a map of records, the record
    consisting of an Long field and a Boolean field. Your code would look
    something like this:

    ---
    GenericRecord[string] m;
    GenericRecord reuse = new GenericRecord();
    for (long i = in.readMapStart(); i != 0; i = in.readMapNext()) {
      for (long j = 0; j < i; j++) {
        string key = in.readString();
        reuse.intField = in.readInt();
        reuse.boolField = in.readBoolean();
        m.put(key, reuse);
      }
    }
    ---

    Throws: AvroTypeException If this is a stateful reader and map is not the
    type of the next value to be read
   */
  abstract size_t readMapStart();

  /**
     Processes the next block of map entries and returns the count of them.
     Similar to [arrayNext]. See [readMapStart] for details.

     Throws: AvroTypeException When called outside of a map context
  */
  abstract size_t readMapNext();

  /**
     Support for quickly skipping through a map similar to [skipArray].

     As an example, let's say you want to skip a map of records, the record
     consisting of an Long field and a Boolean field. Your code would look
     something like this:

     ---
     for (long i = in.skipMap(); i != 0; i = in.skipMap()) {
       for (long j = 0; j < i; j++) {
         in.skipString(); // Discard key
         in.readInt(); // Discard int-field of value
         in.readBoolean(); // Discard boolean-field of value
       }
     }
     ---

     Throws: AvroTypeException If this is a stateful reader and array is not the
     type of the next value to be read
  */
  abstract size_t skipMap();

  /**
     Reads the tag of a union written by [Encoder.writeIndex].

     Throws: AvroTypeException If this is a stateful reader and union is not the
     type of the next value to be read
  */
  abstract size_t readUnionIndex(const Schema unionSchema);

  /// TODO: Document me.
  abstract void readUnionEnd();
}

