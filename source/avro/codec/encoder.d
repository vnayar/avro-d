/// Support logic for serializaing Avro values.
module avro.codec.encoder;

@safe:

/**
   Low-level support for serializing Avro values.

   This class has two types of methods. One type of methods support the writing
   of leaf values (for example, [Encoder.writeLong] and [Encoder.writeString]).
   These methods have analogs in [Decoder].

   The other type of methods support the writing of maps and arrays. These
   methods are [Encoder.writeArrayStart], [Encoder.startItem], and
   [Encoder.writeArrayEnd] (and similar methods for maps). Some implementations
   of [Encoder] handle the buffering required to break large maps and
   arrays into blocks, which is necessary for applications that want to do
   streaming. (See [Encoder.writeArrayStart] for details on these methods.)
*/
abstract class Encoder {

  /**
     "Writes" a null value. (Doesn't actually write anything, but advances the
     state of the parser if this class is stateful.)

     Throws: AvroTypeException If this is a stateful writer and a null is not expected
  */
  abstract void writeNull();

  /**
     Write a boolean value.
     Throws: AvroTypeException If this is a stateful writer and a boolean is not expected
  */
  abstract void writeBoolean(bool b);

  /**
     Writes a 32-bit integer.
     Throws: AvroTypeException If this is a stateful writer and an integer is not expected
  */
  abstract void writeInt(int n);

  /**
     Write a 64-bit integer.
     Throws: AvroTypeException If this is a stateful writer and a long is not expected
  */
  abstract void writeLong(long n);

  /**
     Write a float.
     Throws: AvroTypeException If this is a stateful writer and a float is not expected
  */
  abstract void writeFloat(float f);

  /**
    Write a double.
    Throws: AvroTypeException If this is a stateful writer and a double is not expected
   */
  abstract void writeDouble(double d);

  /**
     Write a Unicode character string.

     Throws: AvroTypeException If this is a stateful writer and a char-string is not expected
  */
  abstract void writeString(string str);
  abstract void writeRecordKey(string key);
  abstract void writeMapKey(string key);

  /**
    Write a byte string.

    Throws: AvroTypeException If this is a stateful writer and a byte-string is not expected
   */
  abstract void writeBytes(ubyte[] bytes, size_t start, size_t len);

  /**
     Writes a byte string. Equivalent to `writeBytes(bytes, 0, bytes.length)`
     Throws: AvroTypeException If this is a stateful writer and a byte-string is not expected
  */
  void writeBytes(ubyte[] bytes) {
    writeBytes(bytes, 0, bytes.length);
  }

  /**
     Writes a fixed size binary object.

     Params:
       bytes = The contents to write
       start = The position within `bytes` where the contents start.
       len   = The number of bytes to write.

     Throws: AvroTypeException If this is a stateful writer and a byte-string is not expected
  */
  abstract void writeFixed(ubyte[] bytes, size_t start, size_t len);

  /**
    A shorthand for `writeFixed(bytes, 0, bytes.length)`.

    Params:
      bytes = The bytes of a fixed value.
   */
  void writeFixed(ubyte[] bytes) {
    writeFixed(bytes, 0, bytes.length);
  }

  /**
    Writes an enumeration.

    Params:
      e   = The ordinal value of an enum to write.
      sym = The textual symbol of the enum.

    Throws: AvroTypeException If this is a stateful writer and an enumeration is not expected or the
        `e` is out of range.
   */
  abstract void writeEnum(size_t e, string sym);

  /**
    Call this method to start writing an array.

    When starting to serialize an array, call [writeArrayStart]. Then,
    before writing any data for any item call [setItemCount] followed by a
    sequence of [startItem()] and the item itself. The number of
    [startItem()] should match the number specified in
    [setItemCount]. When actually writing the data of the item, you can
    call any [Encoder] method (e.g., [writeLong]). When all items of
    the array have been written, call [writeArrayEnd].

    As an example, let's say you want to write an array of records, the record
    consisting of an Long field and a Boolean field. Your code would look
    something like this:

    ---
    out.writeArrayStart();
    out.setItemCount(list.size());
    foreach (GenericRecord r; list) {
      out.startItem();
      out.writeLong(r.longField);
      out.writeBoolean(r.boolField);
    }
    out.writeArrayEnd();
    ---

    Throws: AvroTypeException If this is a stateful writer and an array is not expected
   */
  abstract void writeArrayStart();

  /**
     Call this method before writing a batch of items in an array or a map. Then
     for each item, call [startItem()] followed by any of the other write
     methods of [Encoder]. The number of calls to [startItem()] must
     be equal to the count specified in [setItemCount()]. Once a batch is
     completed you can start another batch with [setItemCount()].

     Params:
     itemCount = The number of [startItem()] calls to follow.
  */
  abstract void setItemCount(size_t itemCount);

  /**
     Start a new item of an array or map. See {@link #writeArrayStart} for usage
     information.

     Throws: AvroTypeException If called outside of an array or map context
  */
  abstract void startItem();

  /**
     Call this method to finish writing an array. See {@link #writeArrayStart} for
     usage information.

     Throws:
     - AvroTypeException If items written does not match count provided to [writeArrayStart].
     - AvroTypeException If not currently inside an array
  */
  abstract void writeArrayEnd();

  /**
     Call this to start a new map. See [writeArrayStart] for details on usage.

     As an example of usage, let's say you want to write a map of records, the
     record consisting of an Long field and a Boolean field. Your code would look
     something like this:

     ---
     out.writeMapStart();
     out.setItemCount(list.size());
     foreach (string key, GenericRecord value; map) {
       out.startItem();
       out.writeString(key);
       out.writeLong(value.getField("longField").getValue!long);
       out.writeBoolean(value.getField("boolField").getValue!bool);
     }
     out.writeMapEnd();
     ---

     Throws: AvroTypeException If this is a stateful writer and a map is not expected
  */
  abstract void writeMapStart();

  /**
     Call this method to terminate the inner-most, currently-opened map. See
     [writeArrayStart] for more details.

     Throws:
     - AvroTypeException If items written does not match count provided to [writeMapStart]
     - AvroTypeException If not currently inside a map
  */
  abstract void writeMapEnd();

  /// TODO: Document me.
  abstract void writeRecordStart();

  /// TODO: Document me.
  abstract void writeRecordEnd();

  abstract void writeUnionStart();
  /**
     Call this method to write the tag of a union.

     As an example of usage, let's say you want to write a union, whose second branch is a record of
     type "thing" consisting of an Long field and a Boolean field. Your code would look something
     like this:

     ---
     out.writeUnionStart();
     out.writeUnionIndex(1, "thing");
     out.writeRecordStart();
     out.writeLong(record.getField("longField").getValue!long);
     out.writeBoolean(record.getField("boolField").getValue!bool);
     out.writeRecordEnd();
     out.writeUnionEnd();
     ---

     Throws: AvroTypeException If this is a stateful writer and a map is not expected
  */
  abstract void writeUnionType(size_t unionTypeIndex, string unionTypeName);

  abstract void writeUnionEnd();

  /// Empty any internal buffers to the underlying output.
  abstract void flush();
}

