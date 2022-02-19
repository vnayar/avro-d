module avro.orderedmap;

import core.exception : AssertError;

/**
   An associative array that permits access to ordered keys.

   This is a thin wrapper around D's built-in
   [Associative Arrays](https://dlang.org/spec/hash-map.html) couple with an
   [Array](https://dlang.org/spec/arrays.html) that keeps track of key order.

   The interface is identical to an Associative Array with one exception: an [OrderedMap]
   may not be initialized from a literal, because the order of keys in a literal is not
   known. E.g. `OrderedMap!(string, int) = ["a": 2, "b": 1];` is not permitted.
 */
struct OrderedMap(KeyT, ValueT) {
  /// The underlying associative array used.
  ValueT[KeyT] map;
  /// A maintained list of keys in the order they were added.
  KeyT[] orderedKeys;

  alias map this;

  /// Hide this method because the order of the initial map is unknown.
  void opAssign(ValueT[KeyT] map) {
    throw new AssertError("Cannot initialize OrderedMap from unordered associative array.");
  }

  /// Assign a value to the map and add it to the orderedKeys if it is new.
  ValueT opIndexAssign(ValueT value, KeyT key) {
    if (key !in map) {
      orderedKeys ~= key;
    }
    map[key] = value;
    return value;
  }

  /// Removes a single item from the map and the orderedKeys.
  void remove(KeyT key) {
    import std.algorithm : remove;
    map.remove(key);
    orderedKeys = orderedKeys.remove!(a => a == key);
  }

  ///
  unittest {
    OrderedMap!(string, int) omap;
    omap["c"] = 1;
    omap["b"] = 2;
    omap["a"] = 3;
    omap.remove("b");
    assert(omap.orderedKeys == ["c", "a"]);
  }

  /// Removes all map keys and clears the orderedKeys.
  void clear() {
    map.clear;
    orderedKeys.length = 0;
  }

  ///
  unittest {
    OrderedMap!(string, int) omap;
    omap["c"] = 1;
    omap["b"] = 2;
    omap["a"] = 3;
    omap.clear();
    assert(omap.orderedKeys == []);
  }
}

/// Initializing from an unordered map is not allowed.
unittest {
  import std.exception : assertThrown;
  OrderedMap!(string, int) omap;
  assertThrown!AssertError(omap = ["a": 1, "b": 2]);
}

/// Values can be read and written just like an associative array.
unittest {
  import std.exception : assertThrown;
  OrderedMap!(string, int) omap;
  omap["b"] = 1;
  assert(omap["b"] == 1);
}

/// A new property `orderedKeys` is available to use.
unittest {
  OrderedMap!(string, int) omap;
  omap["b"] = 1;
  omap["a"] = 2;
  omap["c"] = 3;
  assert(omap.orderedKeys == ["b", "a", "c"]);
}
