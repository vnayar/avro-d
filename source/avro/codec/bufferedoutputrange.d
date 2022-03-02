/// A wrapper around an Output Stream allowing values to buffer in memory before being flushed.
module avro.codec.bufferedoutputrange;

import std.range;
import std.traits;

/// A specialization of std.range.ElementType which also considers output ranges.
template ElementType(R)
if (isFunction!(R.put))
{
  static foreach (t; __traits(getOverloads, R, "put")) {
    static if (!is(done)) {
      static if (Parameters!(t).length == 1 && is(Parameters!(t)[0] T : T[])) {
        alias ElementType = T;
        alias done = bool;
      } else static if (Parameters!(t).length == 1 && is(Parameters!(t)[0] T)) {
        alias ElementType = T;
        alias done = bool;
      }
    }
  }
  static if (!is(done)) {
    alias ElementType = void;
  }
}

unittest {
  struct Ham0(T) {
    void put(T d) {}
  }
  assert(is(ElementType!(Ham0!float) == float));

  class Ham1(T) {
    void put(T[] d) {}
  }
  assert(is(ElementType!(Ham1!float) == float));

  struct Ham2(T) {
    void put() {}
    void put(float f, T[] d) {}
    void put(T[] d) {}
    void put(T d) {}
  }
  assert(is(ElementType!(Ham2!int) == int));

  struct Ham3(T) {
    void put(T[][] d) {}
  }
  assert(is(ElementType!(Ham3!int) == int[]));
}

/// A narrowing of the definition of an output range to only those that could `put(ElemT[])`.
enum bool isBlockOutputRange(ORangeT, ElemT) =
    isOutputRange!(ORangeT, ElemT) && is(typeof(ORangeT.init.put([ ElemT.init ])));

/**
   A buffering output range that writes to another output range in batches.

   Sometimes there are efficiency costs associated with writing a single element at a time. Some
   `OutputRange`s take care of this themselves, and others do not. This is a utility that
   generalizes this buffering logic when it is needed.

   For example, suppose a FileOutputRange has element type `ubyte[]` (it writes many bytes at a
   time). A `BufferedOutputRange` wraps another `OutputRange`, like the FileOutputRange, and permits
   `put` calls of `ubyte` or `ubyte[]` and stores them in a memory buffer. When this buffer is full,
   a single `put(ubyte[])` call will be made with the buffer's contents to the provided
   FileOutputRange.

   The standard OutputRange interface is expanded with `flush()`, which causes the current memory
   buffer's contents to be written immediately to the wrapped `OutputRange`.

   Params:
     ORangeT = The type of the wrapped `OutputRange`, capable of writing batches of elements.
     ElemT   = The type of items in the batch that can be written to ORangeT.
*/
struct BufferedOutputRange(ORangeT, ElemT)
if (isBlockOutputRange!(ORangeT, ElemT))
{
  private ORangeT oRange;
  private size_t bufSize;
  private ElemT[] buf;

  /// Constructs a BufferedOutputRange which writes to `oRange` in batches up to `bufSize`.
  this(ORangeT oRange, size_t bufSize) {
    this.oRange = oRange;
    this.bufSize = bufSize;
    this.buf.reserve(bufSize);
  }

  /// Adds a single element which may be buffered or trigger a flush to the output range.
  void put(ElemT elem) {
    put([elem]);
  }

  /**
     Adds many elements, which may trigger flushes to the output range.

     Writes to the output range may not always be equal to the buffer size if:
     - More elements are being written than the buffer size. The current buffer will be flushed
       and the remaining elements written to the output range.
     - The buffer will not fit the elements. The buffer will be flushed before new elements
       are buffered.
  */
  void put(ElemT[] elems) {
    // Bypass the buffer if writing more than the buffer holds.
    if (elems.length > bufSize) {
      flush();
      oRange.put(elems);
    } else {
      size_t remaining = bufSize - buf.length;
      if (remaining < elems.length)
        flush();
      buf ~= elems.dup;
    }
  }

  /**
     Writes any buffered elements into the underlying output range.
  */
  size_t flush() {
    oRange.put(buf);
    size_t len = buf.length;
    buf.length = 0;
    return len;
  }
}

/**
   A helper function to create a [BufferedOutputRange] with template types inferred from arguments.
   Params:
     oRange  = The OutputRange capable of writing batches of elements, e.g. `.put(T[])`.
     bufSize = The number of elements to buffer in memory before automatically flushing.
     ElemT   = The type of items that can be inserted as batches into ORangeT. This is
               automatically detected if ORangeT has put methods.
               Note: Automatic detection does not work for `std.array.appender.Appender`.
*/
auto bufferedOutputRange(ElemT = ElementType!ORangeT, ORangeT)(
    ORangeT oRange, size_t bufSize = 512) {
  return BufferedOutputRange!(ORangeT, ElemT)(oRange, bufSize);
}

unittest {
  import std.array : appender;
  int[] data = [1];
  auto bufRange = bufferedOutputRange!int(appender(&data), 3);

  bufRange.put(2);
  bufRange.put([3, 4]);
  assert(data == [1]);
  bufRange.put(5);
  assert(data == [1, 2, 3, 4]);
  bufRange.flush();
  assert(data == [1, 2, 3, 4, 5]);
}
