/// Classes used to access generic Avro data using a schema without pre-compiled classes.
module avro.generic.genericdata;

import std.variant : Variant;
import std.conv : to;

import avro.type : Type;
import avro.schema : Schema;
import avro.field : Field;
import avro.exception : AvroRuntimeException;

/**
   GenericDatum which can hold any Avro type. The datum has a type and a value. The type is one of
   the Avro data types. The D type for value corresponds to the Avro type.
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
*/
class GenericDatum {
  private Type type;
  // TODO: Add logical type.
  private Variant value;

  private void initFromSchema(Schema schema) {
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

  /// A constructor allowing GenericDatum to be created for primitive schemas from D equivalents.
  public this(T)(T value) {
    static if (is(T == bool)) {
      this.type = Type.BOOLEAN;
    } else if (is(T == int)) {
      this.type = Type.INT;
    } else if (is(T == long)) {
      this.type = Type.LONG;
    } else if (is(T == float)) {
      this.type = Type.FLOAT;
    } else if (is(T == double)) {
      this.type = Type.DOUBLE;
    } else if (is(T == string)) {
      this.type = Type.STRING;
    } else if (is(T == ubyte[])) {
      this.type = Type.BYTES;
    } else {
      assert(false, "Cannot create primitive GenericDatum from type: " ~ typeid(T).toString);
    }
    this.value = value;
  }

  public this(Schema schema) {
    initFromSchema(schema);
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

  /// ditto
  public void opAssign(T)(T val)
  if (!is(T : GenericDatum)) {
    setValue(val);
  }

  /// For records/maps, looks up a record with [name].
  GenericDatum opIndex(string name) {
    if (type == Type.RECORD) {
      return value.get!(GenericRecord)[name];
    } else {
      throw new AvroRuntimeException("Cannot use [string] operator for type " ~ type.to!string);
    }
  }

  /// Returns true if an only if this datum is a union.
  bool isUnion() {
    return type == Type.UNION;
  }

  /// Returns the index of the current branch, if this is a union.
  size_t getUnionIndex()
  in (isUnion(), "Cannot get union index on type: " ~ type.to!string)
  {
    return value.get!GenericUnion().getUnionIndex();
  }

  /// Selects a new branch in the union if this is a union.
  void setUnionIndex(size_t branch)
  in (isUnion(), "Cannot set union index on type: " ~ type.to!string)
  {
    value.get!GenericUnion().setUnionIndex(branch);
  }
}

/// The base class for all generic types that act as containers.
class GenericContainer {
  private Schema schema;

  /// Validates that a given schema is of the expected type.
  private void assertType(Schema schema, Type type) {
    if (schema.getType() != type) {
      throw new AvroRuntimeException("Schema has type " ~ schema.getType().to!string
          ~ ", but expected type " ~ type.to!string);
    }
  }
  protected this(Type type, Schema schema) {
    this.schema = schema;
    assertType(schema, type);
  }

  public Schema getSchema() {
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
  this(Schema schema) {
    super(Type.UNION, schema);
    setUnionIndex(0);
  }

  /// Returns the index of the current branch.
  size_t getUnionIndex() {
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
  this(Schema schema) {
    super(Type.RECORD, schema);
    Field[] schemaFields = schema.getFields();
    fieldData.length = schemaFields.length;
    foreach (size_t i, Field field; schemaFields) {
      fieldData[i] = new GenericDatum(field.getSchema());
    }
  }

  /// Returns the number of fields in the current record.
  size_t fieldCount() {
    return fieldData.length;
  }

  /// Returns index of the field with the given name.
  size_t fieldIndex(string name) {
    size_t index = 0;
    Field field = getSchema().getField(name);
    if (field is null) {
      throw new AvroRuntimeException("Invalid field name: " ~ name);
    }
    return field.getPosition();
  }

  /// Returns true if this record has a field with the given name.
  bool hasField(string name) {
    return getSchema().getField(name) !is null;
  }

  /// Returns the field data with the given name.
  GenericDatum getField(string name) {
    return fieldAt(fieldIndex(name));
  }

  /// ditto
  GenericDatum opIndex(string name) {
    return getField(name);
  }

  /// Returns the field data at the given position.
  GenericDatum fieldAt(size_t pos) {
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
  this(Schema schema) {
    super(Type.ARRAY, schema);
  }

  /// Returns the contents of the array.
  ref GenericDatum[] getValue() return {
    return value;
  }
}

/// A generic container for Avro maps.
class GenericMap : GenericContainer {
  private GenericDatum[string] value;

  /// Constructs a generic map according to the given map-type schema.
  this(Schema schema) {
    super(Type.MAP, schema);
  }

  /// Returns the data contents of the map.
  GenericDatum[string] getValue() {
    return value;
  }
}

/// A generic container for Avro enums.
class GenericEnum : GenericContainer {
  private size_t value;

  private size_t getEnumOrdinal(Schema schema, string symbol) {
    if (getSchema().hasEnumSymbol(symbol))
      return getSchema().getEnumOrdinal(symbol);
    throw new AvroRuntimeException("No such symbol: " ~ symbol);
  }

  /// Constructs a generic enum according to the given enum-type schema.
  this(Schema schema) {
    super(Type.ENUM, schema);
  }

  this(Schema schema, string symbol) {
    this(schema);
    value = GenericEnum.getEnumOrdinal(schema, symbol);
  }

  /**
     Returns the symbol corresponding to ordinal n.
     Throws: AvroRuntimeException if the enum has no such ordinal.
  */
  string getSymbol(size_t n) {
    string[] symbols = getSchema().getEnumSymbols();
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
  size_t getEnumOrdinal(string symbol) {
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
  size_t getValue() {
    return value;
  }

  /// Returns the symbol for the current value of this enum.
  string getSymbol() {
    string[] symbols = getSchema().getEnumSymbols();
    return symbols[value];
  }
}

/// A generic container for Avro fixed.
class GenericFixed : GenericContainer {
  private ubyte[] value;

  /// Constructs a generic fixed value according to the given fixed-type Avro schema.
  this(Schema schema) {
    super(Type.FIXED, schema);
    value.length = schema.getFixedSize();
  }

  this(Schema schema, ubyte[] v) {
    this(schema);
    value = v;
  }

  ubyte[] getValue() {
    return value;
  }
}
