module avro.attributes;

/**
   This template can be used to add functionality to a class by inserting class members
   and functions so that the object can represent an arbitrary list of JSON attributes.

   To use this mixing, simply call it by name in your class:
   ```
   class MyNode {
     mixin HasJsonAttributes;
   }
   ```
*/
mixin template HasJsonAttributes() {
  import std.json : JSONValue;
  import avro.exception : AvroRuntimeException;
  import avro.orderedmap : OrderedMap;

  private OrderedMap!(string, JSONValue) attributes;

  /**
     Adds a property with the given name [name] and value [value].
     Neither [name] nor [value] can be [null]. It is illegal
     to add a property if another with the same name but different value already
     exists in this schema.

     Params:
       name  = The name of the property to add
       value = The value for the property to add
  */
  public void addAttribute(T)(string name, T value) {
    if (name in attributes)
      throw new AvroRuntimeException("Can't overwrite property: " ~ name);
    attributes[name] = JSONValue(value);
  }

  /// Retrieve a map from JSON attribute names to their JSONValues.
  OrderedMap!(string, JSONValue) getAttributes() {
    return attributes;
  }
}

///
unittest {
  import std.exception : assertThrown;
  import std.json : JSONValue;
  import avro.exception : AvroRuntimeException;

  class Thing {
    mixin HasJsonAttributes;
  }

  Thing thing = new Thing();
  thing.addAttribute("abe", 3);
  thing.addAttribute("bob", "ham");

  assert(thing.getAttributes().length == 2);
  assert(thing.getAttributes()["bob"] == JSONValue("ham"));
  assert(thing.getAttributes().orderedKeys == ["abe", "bob"]);

  assertThrown!AvroRuntimeException(thing.addAttribute("abe", 4));
}
