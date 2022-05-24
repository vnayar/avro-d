/// Classes used to access generic Avro data using a schema without pre-compiled classes.
module avro.generic.genericdata;

import std.traits;
import std.variant : Variant, VariantException;
import std.conv : to;

import avro.type : Type;
import avro.schema : Schema;
import avro.field : Field;
import avro.exception : AvroRuntimeException;

/**
   GenericDatum which can hold any Avro type. The datum has a type and a value.

   The type is one of the Avro data types. The D type for value corresponds to the Avro type.
   - An avro `null` corresponds to no D type. It is illegal to try to access values for `null`.
   - Avro `boolean` maps to D `bool`
   - Avro `int` maps to D `int`.
   - Avro `long` maps to D `long`.
   - Avro `float` maps to D `float`.
   - Avro `double` maps to D `double`.
   - Avro `string` maps to D `string`.
   - Avro `bytes` maps to D `ubyte[]`.
   - Avro `fixed` maps to D class `GenericFixed`.
   - Avro `enum` maps to D class `GenericEnum`.
   - Avro `array` maps to D class `GenericArray`.
   - Avro `map` maps to D class `GenericMap`.
   - There is no D type corresponding to Avro `union`. The object should have the D type
     corresponding to one of the constituent types of the union.

   Each GenericDatum holds a value which is set using the `.setValue(T)(T val)` method and retrieved
   via the `.getValue!T() method. Because a GenericDatum can store any type, the caller must provide
   the desired type while calling `.getValue!T()`, and this type must match the type of the schema.

   ---
  Schema schema = parser.parseText(q"EOS
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number", "type": ["int", "null"]},
     {"name": "scores", "type": {"type": "array", "items": "float"}},
     {"name": "m", "type": {"type": "map", "values": "long"}}
 ]
}
EOS");

   // Initializes the GenericDatum according to the schema with default values.
   GenericDatum datum = new GenericDatum(schema);
   assert(datum.getType == Type.RECORD);

   // Primitive values can be set and retrieved.
   datum.getValue!(GenericRecord).getField("name").setValue("bob");

   // Convenience shortcut using opIndex() and opAssign() for primitive types.
   datum["name"] = "bob";

   assert(datum["name"].getValue!string == "bob");

   // Unions have convenience functions directly on GenericData.
   datum["favorite_number"].setUnionIndex(0);
   assert(datum["favorite_number"].getUnionIndex() == 0);

   // Arrays also have convenience functions.
   datum["scores"] ~= 1.23f;
   datum["scores"] ~= 4.56f;
   assert(datum["scores"].length == 2);

   // Maps do as well.
   datum["m"]["m1"] = 10L;
   datum["m"]["m2"] = 20L;
   assert(datum["m"]["m1"].getValue!long == 10L);
   ---
*/
class GenericDatum {
  private Type type;
  // TODO: Add logical type.
  private Variant value;

  private void initFromSchema(const Schema schema) {
    type = schema.getType();
    final switch (type) {
      case Type.NULL:
        break;
      case Type.BOOLEAN:
        value = bool.init;
        break;
      case Type.INT:
        value = int.init;
        break;
      case Type.LONG:
        value = long.init;
        break;
      case Type.FLOAT:
        value = float.init;
        break;
      case Type.DOUBLE:
        value = double.init;
        break;
      case Type.BYTES:
        value = ubyte.init;
        break;
      case Type.STRING:
        value = string.init;
        break;
      case Type.RECORD:
        value = new GenericRecord(schema);
        break;
      case Type.ENUM:
        value = new GenericEnum(schema);
        break;
      case Type.ARRAY:
        value = new GenericArray(schema);
        break;
      case Type.MAP:
        value = new GenericMap(schema);
        break;
      case Type.UNION:
        value = new GenericUnion(schema);
        break;
      case Type.FIXED:
        value = new GenericFixed(schema);
        break;
    }
  }

  /// Makes a new NULL GenericDatum.
  public this() {
    this.type = Type.NULL;
  }

  public this(const Schema schema) {
    initFromSchema(schema);
  }

  /// A constructor allowing GenericDatum to be created for primitive schemas from D equivalents.
  public this(T)(T val)
  if (!is(T : Schema))
  {
    this.value = val;
    static if (is(T : bool)) {
      this.type = Type.BOOLEAN;
    } else static if (is(T : int)) {
      this.type = Type.INT;
    } else static if (is(T : long)) {
      this.type = Type.LONG;
    } else static if (is(T : float)) {
      this.type = Type.FLOAT;
    } else static if (is(T : double)) {
      this.type = Type.DOUBLE;
    } else static if (is(T : string)) {
      this.type = Type.STRING;
    } else static if (is(T : ubyte[])) {
      this.type = Type.BYTES;
    } else {
      assert(false, "Cannot create primitive GenericDatum from type: " ~ typeid(T).toString);
    }
  }

  public inout(Type) getType() inout {
    return type == Type.UNION
        ? value.get!(GenericUnion).getDatum().getType()
        : type;
  }

  /**
     Returns the value held by this datum.
     Params:
       T = The type of the value, which much correspond to the Avro type returned by [getType()].
  */
  public inout(T) getValue(T)() inout {
    return type == Type.UNION
        ? value.get!(GenericUnion).getDatum().getValue!T()
        : value.get!T;
  }

  /// Implementing opCast allows values to be retrieved using `std.conv.to`.
  public inout(T) opCast(T)() inout {
    return getValue!T();
  }

  /**
     Sets the value of the GenericDatum to a value corresponding with its type.

     Throws: VariantException when the value type does not match the datum type.
  */
  public void setValue(T)(T val) {
    if (type == Type.UNION) {
      value.get!(GenericUnion).getDatum().setValue(val);
    } else {
      value = val;
    }
  }

  public void setValue(string val) {
    if (type == Type.UNION) {
      value.get!(GenericUnion).getDatum().setValue(val);
    } else if (type == Type.ENUM) {
      value.get!(GenericEnum).setSymbol(val);
    } else {
      value = val;
    }
  }

  /// ditto
  public void opAssign(T)(T val)
  if (!is(T : GenericDatum)) {
    setValue(val);
  }

  void opOpAssign(string op, T)(T val)
  if (op == "~" && (isBasicType!T || isSomeString!T))
  {
    if (type == Type.ARRAY) {
      static if (isBasicType!T || isSomeString!T) {
        value.get!(GenericArray).getValue() ~= new GenericDatum(val);
      } else {
        value.get!(GenericArray).getValue() ~= val;
      }
    } else {
      throw new AvroRuntimeException("Cannot use ~= operator for type " ~ type.to!string);
    }
  }

  /// For records/maps, looks up a record with `name`.
  ref inout(GenericDatum) opIndex(string name) inout {
    if (type == Type.RECORD) {
      return (value.get!(GenericRecord))[name];
    } else if (type == Type.MAP) {
      return (value.get!(GenericMap))[name];
    } else {
      // Bug with std.conv:to and inout, See https://issues.dlang.org/show_bug.cgi?id=20623
      throw new AvroRuntimeException("Only RECORD and MAP types can use the [string] operator.");
    }
  }

  /// For records/maps, looks up a record with `name`.
  ref inout(GenericDatum) opIndex(int idx) inout {
    if (type == Type.ARRAY) {
      return value.get!(GenericArray)[idx];
    } else {
      throw new AvroRuntimeException("Only ARRAY types can use the [int] operator.");
    }
  }

  /// For records/maps, assign a value to a given key.
  void opIndexAssign(T)(T val, string name) {
    if (type == Type.RECORD) {
      (value.get!GenericRecord)[name] = val;
    } else if (type == Type.MAP) {
      (value.get!GenericMap)[name] = val;
    } else {
      throw new AvroRuntimeException("Cannot use [string] assignment for type " ~ type.to!string);
    }

  }

  /// For arrays, assign a value to a given index.
  void opIndexAssign(T)(T val, int idx) {
    if (type == Type.ARRAY) {
      value.get!(GenericArray)[idx] = val;
    } else {
      throw new AvroRuntimeException("Cannot use [string] assignment for type " ~ type.to!string);
    }
  }

  /// Returns true if an only if this datum is a union.
  bool isUnion() const {
    return type == Type.UNION;
  }

  /// Returns the index of the current branch, if this is a union.
  size_t getUnionIndex() const
  in (isUnion(), "Cannot get union index on type: " ~ type.to!string)
  {
    return value.get!GenericUnion().getUnionIndex();
  }

  const(Schema) getUnionSchema() const
  in (isUnion(), "Cannot get union schema on type: " ~ type.to!string)
  {
    return value.get!GenericUnion().getSchema();
  }

  /// Selects a new branch in the union if this is a union.
  void setUnionIndex(size_t branch)
  in (isUnion(), "Cannot set union index on type: " ~ type.to!string)
  {
    value.get!GenericUnion().setUnionIndex(branch);
  }

  /**
     A shortcut for .getValue!(GenericType).getValue().length where GenericType is one of
     GenericArray or GenericMap.
  */
  size_t length() const {
    if (type == Type.ARRAY) {
      return value.get!(GenericArray).length;
    } else if (type == Type.MAP) {
      return value.get!(GenericMap).length;
    } else {
      throw new AvroRuntimeException("Cannot use .length() for type " ~ type.to!string);
    }
  }

  override
  string toString() const {
    switch (type) {
      case Type.NULL:
        return "null";
      case Type.BOOLEAN:
        return getValue!bool() ? "true" : "false";
      case Type.INT:
        return getValue!int().to!string();
      case Type.LONG:
        return getValue!long().to!string();
      case Type.FLOAT:
        return getValue!float().to!string();
      case Type.DOUBLE:
        return getValue!double().to!string();
      case Type.STRING:
        return "\"" ~ getValue!string() ~ "\"";
      default:
        return this.classinfo.name;
    }
  }
}

/// The base class for all generic types that act as containers.
class GenericContainer {
  private const(Schema) schema;

  /// Validates that a given schema is of the expected type.
  private void assertType(const Schema schema, Type type) {
    if (schema.getType() != type) {
      throw new AvroRuntimeException("Schema has type " ~ schema.getType().to!string
          ~ ", but expected type " ~ type.to!string);
    }
  }
  protected this(Type type, const Schema schema) {
    this.schema = schema;
    assertType(schema, type);
  }

  public const(Schema) getSchema() const {
    return schema;
  }
}

/**
   A generic container for unions.

   A union consists of several types, e.g. \["null", "int", "string" \], however, only a single
   union type is represented in data at any given moment. That is, an instance of the above union
   may contain the datum `3` or `"apple"` but not both.
*/
class GenericUnion : GenericContainer {
  private int unionIndex;
  private GenericDatum datum;

  /**
     Constructs a generic union corresponding to the given schema and the given value. The schema
     should be of Avro type union and the value should correspond to one of the union types.
  */
  this(const Schema schema) {
    super(Type.UNION, schema);
    setUnionIndex(0);
  }

  /// Returns the index of the current branch.
  size_t getUnionIndex() const {
    return unionIndex;
  }

  /**
     Selects a new branch. The type for the value is changed accordingly.
     Params:
       index = The index for the selected branch.
  */
  void setUnionIndex(size_t index) {
    if (unionIndex != index || datum is null) {
      datum = new GenericDatum(getSchema().getTypes()[index]);
      unionIndex = index.to!int;
    }
  }

  /// Returns the datum corresponding to the currently selected union type.
  inout(GenericDatum) getDatum() inout {
    return datum;
  }
}

/// The generic container for Avro records.
class GenericRecord : GenericContainer {
  private GenericDatum[] fieldData;

  /// Constructs a generic record corresponding to the given "record" type schema.
  this(const Schema schema) {
    super(Type.RECORD, schema);
    const(Field[]) schemaFields = schema.getFields();
    fieldData.length = schemaFields.length;
    foreach (size_t i, const(Field) field; schemaFields) {
      fieldData[i] = new GenericDatum(field.getSchema());
    }
  }

  /// Returns the number of fields in the current record.
  size_t fieldCount() {
    return fieldData.length;
  }

  /// Returns index of the field with the given name.
  size_t fieldIndex(string name) const {
    auto field = getSchema().getField(name);
    return field.getPosition();
  }

  /// Returns the field data with the given name.
  ref inout(GenericDatum) getField(string name) inout {
    return fieldAt(fieldIndex(name));
  }

  /// ditto
  ref inout(GenericDatum) opIndex(string name) inout {
    return getField(name);
  }

  /// Returns the field data at the given position.
  ref inout(GenericDatum) fieldAt(size_t pos) inout {
    return fieldData[pos];
  }

  /// Replace the field data at the given position.
  void setFieldAt(size_t pos, GenericDatum v) {
    fieldData[pos] = v;
  }
}

/// A generic container for Avro arrays.
class GenericArray : GenericContainer {
  private GenericDatum[] value;

  /// Constructs a generic array according to the given array-type schema.
  this(const Schema schema) {
    super(Type.ARRAY, schema);
  }

  /// Returns the contents of the array.
  ref GenericDatum[] getValue() return {
    return value;
  }

  /// ditto
  const(GenericDatum[]) getValue() const {
    return value;
  }

  ref inout(GenericDatum) opIndex(int idx) inout {
    return value[idx];
  }

  void opIndexAssign(T)(T val, int idx)
  if (isBasicType!T || isSomeString!T) {
    value[idx] = new GenericDatum(val);
  }

  size_t length() const {
    return value.length;
  }
}

/// A generic container for Avro maps.
class GenericMap : GenericContainer {
  private GenericDatum[string] value;

  /// Constructs a generic map according to the given map-type schema.
  this(const Schema schema) {
    super(Type.MAP, schema);
  }

  /// Returns the data contents of the map.
  ref GenericDatum[string] getValue() return {
    return value;
  }

  const(GenericDatum[string]) getValue() const {
    return value;
  }

  ref inout(GenericDatum) opIndex(string name) inout {
    return value[name];
  }

  void opIndexAssign(T)(T val, string name)
  if (isBasicType!T || isSomeString!T) {
    value[name] = new GenericDatum(val);
  }

  size_t length() const {
    return value.length;
  }
}

/// A generic container for Avro enums.
class GenericEnum : GenericContainer {
  private size_t value;

  private size_t getEnumOrdinal(const Schema schema, string symbol) const {
    if (getSchema().hasEnumSymbol(symbol))
      return getSchema().getEnumOrdinal(symbol);
    throw new AvroRuntimeException("No such symbol: " ~ symbol);
  }

  /// Constructs a generic enum according to the given enum-type schema.
  this(const Schema schema) {
    super(Type.ENUM, schema);
  }

  this(const Schema schema, string symbol) {
    this(schema);
    value = GenericEnum.getEnumOrdinal(schema, symbol);
  }

  /**
     Returns the symbol corresponding to ordinal n.
     Throws: AvroRuntimeException if the enum has no such ordinal.
  */
  string getSymbol(size_t n) const {
    const(string[]) symbols = getSchema().getEnumSymbols();
    if (n < symbols.length) {
      return symbols[n];
    }
    throw new AvroRuntimeException("No enum symbol at ordinal " ~ n.to!string);
  }

  /// Set the value for this enum according to the given symbol.
  size_t setSymbol(string symbol) {
    return value = getEnumOrdinal(symbol);
  }

  /**
     Returns the ordinal for the given symbol.
     Throws: AvroRuntimeException if the symbol does not match any enum value.
  */
  size_t getEnumOrdinal(string symbol) const {
    return GenericEnum.getEnumOrdinal(getSchema(), symbol);
  }

  /// Set the value for the enum according to the given ordinal.
  void setEnumOrdinal(size_t n) {
    if (n < getSchema().getEnumSymbols().length) {
      value = n;
      return;
    }
    throw new AvroRuntimeException("No enum symbol at ordinal " ~ n.to!string);
  }

  /// Returns the ordinal for the current value of the enum.
  size_t getValue() const {
    return value;
  }

  /// Returns the symbol for the current value of this enum.
  string getSymbol() const {
    const(string[]) symbols = getSchema().getEnumSymbols();
    return symbols[value];
  }
}

/// A generic container for Avro fixed.
class GenericFixed : GenericContainer {
  private ubyte[] value;

  /// Constructs a generic fixed value according to the given fixed-type Avro schema.
  this(const Schema schema) {
    super(Type.FIXED, schema);
    value.length = schema.getFixedSize();
  }

  this(const Schema schema, ubyte[] v) {
    this(schema);
    value = v;
  }

  ref inout(ubyte[]) getValue() inout {
    return value;
  }

  void setValue(ubyte[] val) {
    value = val;
  }
}
