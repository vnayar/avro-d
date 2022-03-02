module avro.codec.bufferedinputrange;

import std.range;

/// A utility to determine if an input range can return blocks of elements at a time.
enum bool isBlockInputRange(IRangeT, ElemT = ElementType!(ElementType!IRangeT)) =
    isInputRange!(IRangeT)
    && isInputRange!(ElementType!IRangeT)
    && is(ElementType!(ElementType!IRangeT) : ElemT);

unittest {
  assert(isBlockInputRange!(int[]) == false);
  assert(isBlockInputRange!(int[][]) == true);

  class BlockInputC {
    int[] front() { return []; }
    bool empty() { return true; }
    void popFront() {}
  }
  assert(isBlockInputRange!(BlockInputC) == true);
  assert(isBlockInputRange!(BlockInputC, int) == true);
  assert(isBlockInputRange!(BlockInputC, char) == false);
  assert(isBlockInputRange!(BlockInputC, float) == true); // type-promotion

  struct BlockInputS {
    float[] front() { return []; }
    bool empty() { return true; }
    void popFront() {}
  }
  assert(isBlockInputRange!BlockInputS == true);
  assert(isBlockInputRange!(BlockInputS, float) == true);
  assert(isBlockInputRange!(BlockInputS, int) == false);
  assert(isBlockInputRange!(BlockInputS, double) == true); // type-promotion
}

/**
   A buffering input range that reads chunks of data from an input range and allows them to be read
   one at a time.

   Sometimes there are high overhead costs associated with reading from certain data sources. To
   avoid performance problems, it is frequently better to read blocks of data all at once, and then
   use that buffered data to serve requests for individual data items.

   Params:
     IRangeT = The type of the wrapped `InputRange`, capable of providing batches of elements.
     ElemT   = The type of items in the batch that can be read from the IRangeT.
*/
struct BufferedInputRange(IRangeT, ElemT)
if (isBlockInputRange!(IRangeT, ElemT))
{
  private IRangeT iRange;
  private typeof(IRangeT.init.front) buf;

  /**
     Constructs a BufferedInputRange which reads blocks from `iRange` but serves
     the data one element at a time like a normal range.
  */
  this(IRangeT iRange) {
    this.iRange = iRange;
  }

  /// Extracts the next element in the range.
  ElemT front() {
    if (buf.empty())
      more();
    return buf.front;
  }

  /// Indicates whether any elements remain in the range.
  bool empty() {
    return buf.empty() && iRange.empty();
  }

  /// Move the input range to the next element.
  void popFront() {
    ElemT val = front();
    buf.popFront();
  }

  private void more() {
    if (!iRange.empty()) {
      buf = iRange.front();
      iRange.popFront();
    }
  }
}

/// A convenience method for creating a [BufferedInputRange] with tempate arguments inferred.
auto bufferedInputRange(ElemT = ElementType!(ElementType!IRangeT), IRangeT)(IRangeT iRange) {
  return BufferedInputRange!(IRangeT, ElemT)(iRange);
}

///
unittest {
  import std.exception : assertThrown;
  import core.exception : AssertError;

  int i = 1;
  auto bulkIRange = generate!(() => repeat(i++, 2)).take(3);
  auto bufferedIRange = bufferedInputRange!int(bulkIRange);

  assert(bufferedIRange.front == 1);
  assert(bufferedIRange.empty == false);
  bufferedIRange.popFront();
  assert(bufferedIRange.front == 1);
  bufferedIRange.popFront();
  assert(bufferedIRange.front == 2);
  bufferedIRange.popFront();
  assert(bufferedIRange.front == 2);
  assert(bufferedIRange.empty == false);
  bufferedIRange.popFront();
  assert(bufferedIRange.front == 3);
  bufferedIRange.popFront();
  assert(bufferedIRange.front == 3);
  bufferedIRange.popFront();
  assert(bufferedIRange.empty == true);

  assertThrown!AssertError(bufferedIRange.front());
}
