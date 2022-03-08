/// Schemas describe the valid format of data, used for (en/de)coding, code generation, and more.
module avro.schema;

import std.conv : to;
import std.json : JSONValue, JSONType, parseJSON;
import std.stdio : stderr;

import avro.field : Field;
import avro.type : Type, PRIMITIVE_TYPE_BY_NAME;
import avro.name : Name;
import avro.attributes : HasJsonAttributes;
import avro.orderedmap : OrderedMap;
import avro.exception : AvroRuntimeException, AvroTypeException, SchemaParseException;

version (unittest) {
  import std.exception : assertThrown, assertNotThrown;
}

/**
   An Avro Schema is one of the following:
   - A JSON string, matching a defined type like "int", "string", or another Schema's name.
   - A JSON object, of the form `{"type": "typeName", ...attributes...}`.
   - A JSON array of type names like `["null", "string"]`, representing a union where values
     adhering to the schema may be one of the listed types.

   It should be noted that certain types can contain schemas via their attributes, making
   a Schema a tree-like structure with each Schema having potential nested schemas.

   See_Also: https://avro.apache.org/docs/current/spec.html#schemas
*/
public abstract class Schema {

  /// Every schema has, at a minimum, a type attribute.
  private immutable(Type) type;
  /// TODO: Add more logic here.
  package string logicalType = null;

  /// Avro schemas are permitted to metadat attributes that do not impact serialized data.
  /// The JSON from the schema is transparently passed through without further analysis.
  mixin HasJsonAttributes;

  // TODO: Add Logical type

  package this(Type type) {
    this.type = type;
  }

  /// Creates a schema given a primitive type.
  public static Schema createPrimitive(Type type) {
    switch (type) {
      case Type.NULL:
        return new NullSchema();
      case Type.BOOLEAN:
        return new BooleanSchema();
      case Type.INT:
        return new IntSchema();
      case Type.LONG:
        return new LongSchema();
      case Type.FLOAT:
        return new FloatSchema();
      case Type.DOUBLE:
        return new DoubleSchema();
      case Type.BYTES:
        return new BytesSchema();
      case Type.STRING:
        return new StringSchema();
      default:
        throw new AvroRuntimeException("Cannot create a '" ~ type.to!string ~ "' schema.");
    }
  }

  package static JSONValue validateDefault(
      string fieldName, Schema schema, JSONValue defaultValue) {
    if (!defaultValue.isNull() && !isValidDefault(schema, defaultValue)) {
      string message = "Invalid default for field " ~ fieldName ~ ": " ~ defaultValue.toString
          ~ " not a " ~ schema.toString;
      throw new AvroTypeException(message);
    }
    return defaultValue;
  }

  unittest {
    import std.exception : assertThrown, assertNotThrown;
    import std.json : parseJSON;

    // Valid string defaults.
    auto stringSchema = Schema.createPrimitive(Type.STRING);
    assertNotThrown(validateDefault("a", stringSchema, JSONValue("fish")));
    assertThrown!AvroTypeException(validateDefault("a", stringSchema, JSONValue(3)));
    assertThrown!AvroTypeException(validateDefault("a", stringSchema, JSONValue(["a"])));


    // Valid integer defaults.
    auto intSchema = Schema.createPrimitive(Type.INT);
    assertNotThrown(validateDefault("b", intSchema, JSONValue(1234)));
    assertThrown!AvroTypeException(validateDefault("b", intSchema, JSONValue(4294967296)));
    assertThrown!AvroTypeException(validateDefault("b", intSchema, JSONValue("bear")));

    // Valid long defaults
    auto longSchema = Schema.createPrimitive(Type.LONG);
    assertNotThrown(validateDefault("c", longSchema, JSONValue(4294967296)));
    assertThrown!AvroTypeException(validateDefault("c", longSchema, JSONValue([1])));
    assertThrown!AvroTypeException(validateDefault("c", longSchema, JSONValue("bear")));

    // Valid float/double defaults
    auto floatSchema = Schema.createPrimitive(Type.FLOAT);
    assertNotThrown(validateDefault("d", floatSchema, JSONValue(4294967296.123112431)));
    assertThrown!AvroTypeException(validateDefault("d", floatSchema, JSONValue(1234)));
    assertThrown!AvroTypeException(validateDefault("d", floatSchema, JSONValue("bear")));

    // Valid boolean defaults
    auto boolSchema = Schema.createPrimitive(Type.BOOLEAN);
    assertNotThrown(validateDefault("e", boolSchema, JSONValue(true)));
    assertNotThrown(validateDefault("e", boolSchema, JSONValue(false)));
    assertThrown!AvroTypeException(validateDefault("e", boolSchema, JSONValue(1)));

    // Valid null defaults
    auto nullSchema = Schema.createPrimitive(Type.NULL);
    assertNotThrown(validateDefault("f", nullSchema, JSONValue(null)));
    assertThrown!AvroTypeException(validateDefault("f", nullSchema, JSONValue(1)));

    // With arrays, defaults must match the array type.
    auto arraySchema = new ArraySchema(Schema.createPrimitive(Type.INT));
    assertNotThrown(validateDefault("g", arraySchema, JSONValue([3, 4])));
    assertThrown!AvroTypeException(validateDefault("g", arraySchema, JSONValue(["a"])));
    assertThrown!AvroTypeException(validateDefault("g", arraySchema, JSONValue("a")));

    // With maps, defaults must match the value type.
    auto mapSchema = new MapSchema(Schema.createPrimitive(Type.INT));
    assertNotThrown(validateDefault("h", mapSchema, JSONValue(["a": 3])));
    assertThrown!AvroTypeException(validateDefault("h", mapSchema, JSONValue([3, 4])));
    assertThrown!AvroTypeException(validateDefault("h", mapSchema, JSONValue("a")));

    // With unions, the default value must match the first type in the union.
    auto unionSchema =
        new UnionSchema([Schema.createPrimitive(Type.STRING), Schema.createPrimitive(Type.INT)]);
    assertNotThrown(validateDefault("i", unionSchema, JSONValue("a")));
    assertThrown!AvroTypeException(validateDefault("i", unionSchema, JSONValue(["a": 3])));
    assertThrown!AvroTypeException(validateDefault("i", unionSchema, JSONValue(3)));

    // With records, each field has its own schema-appropriate default.
    auto recordSchema =
        new RecordSchema(new Name("record", null), "", false, [
            new Field("a", intSchema, "", JSONValue(3), true, Field.Order.IGNORE),
            new Field("b", stringSchema, "", JSONValue("ab"), true, Field.Order.IGNORE)
        ]);
    assertNotThrown(validateDefault("i", recordSchema, parseJSON(`{"a": 3, "b": "ab"}`)));
    assertThrown!AvroTypeException(
        validateDefault("i", recordSchema, JSONValue(["a": "ab", "b": "ab"])));
    assertThrown!AvroTypeException(validateDefault("i", recordSchema, JSONValue(3)));
  }

  private static bool isValidDefault(const Schema schema, JSONValue defaultValue) {
    switch (schema.getType()) {
      case Type.STRING:
      case Type.BYTES:
      case Type.ENUM:
      case Type.FIXED:
        return defaultValue.type == JSONType.string;
      case Type.INT:
        return (defaultValue.type == JSONType.integer && defaultValue.integer < int.max)
            || (defaultValue.type == JSONType.uinteger && defaultValue.uinteger < uint.max);
      case Type.LONG:
        return defaultValue.type == JSONType.integer || defaultValue.type == JSONType.uinteger;
      case Type.FLOAT:
      case Type.DOUBLE:
        return defaultValue.type == JSONType.float_;
      case Type.BOOLEAN:
        return defaultValue.type == JSONType.true_ || defaultValue.type == JSONType.false_;
      case Type.NULL:
        return defaultValue.isNull();
      case Type.ARRAY:
        if (defaultValue.type != JSONType.array)
          return false;
        foreach (JSONValue element; defaultValue.array)
          if (!isValidDefault(schema.getElementSchema(), element))
            return false;
        return true;
      case Type.MAP:
        if (defaultValue.type != JSONType.object)
          return false;
        foreach (JSONValue value; defaultValue.object)
          if (!isValidDefault(schema.getValueSchema(), value))
            return false;
        return true;
      case Type.UNION: // union default: first branch
        return isValidDefault(schema.getTypes()[0], defaultValue);
      case Type.RECORD:
        if (defaultValue.type != JSONType.object)
          return false;
        foreach (const Field field; schema.getFields()) {
          if (!isValidDefault(
                  field.schema,
                  field.name in defaultValue.object
                      ? defaultValue.object[field.name] : field.defaultValue))
            return false;
        }
        return true;
      default:
        return false;
    }
  }

  /// Return the type of this schema.
  public Type getType() const {
    return type;
  }

  /**
     Return the logical type, which can be combined with a type for special interpretation,
     like a timestamp or a date.
  */
  public string getLogicalType() const {
    return logicalType;
  }

  /**
    If this is a record, returns the Field with the given name [fieldName]. If there is no field by
    that name, a [null] is returned.
  */
  public const(Field) getField(string fieldName) const {
    throw new AvroRuntimeException("Not a record: " ~ this.toString);
  }

  /**
    If this is a record, returns the fields in it. The returned list is in the order of their
    positions.
  */
  public const(Field[]) getFields() const {
    throw new AvroRuntimeException("Not a record: " ~ this.toString);
  }

  /**
     If this is a record, set its fields. The fields can be set only once in a
     schema.
  */
  public void setFields(Field[] fields) {
    throw new AvroRuntimeException("Not a record: " ~ this.toString);
  }

  /// If this is an enum, return its symbols.
  public const(string[]) getEnumSymbols() const {
    throw new AvroRuntimeException("Not an enum: " ~ this.toString);
  }

  /// If this is an enum, return its default value.
  public string getEnumDefault() {
    throw new AvroRuntimeException("Not an enum: " ~ this.toString);
  }

  /// If this is an enum, return a symbol's ordinal value.
  public size_t getEnumOrdinal(string symbol) const {
    throw new AvroRuntimeException("Not an enum: " ~ this.toString);
  }

  /// If this is an enum, returns true if it contains given symbol.
  public bool hasEnumSymbol(string symbol) const {
    throw new AvroRuntimeException("Not an enum: " ~ this.toString);
  }

  /**
     If this is a record, enum, or fixed, return its name, otherwise return the name of
     the primitive type.
  */
  public string getName() {
    import std.uni : toLower;
    return type.to!string.toLower;
  }

  /**
     If this is a record, enum, or fixed, returns its docstring, if available.
     Otherwise, returns null.
  */
  public string getDoc() {
    return null;
  }

  /** If this is a record, enum or fixed, returns its namespace, if any. */
  public string getNamespace() {
    throw new AvroRuntimeException("Not a named type: " ~ typeof(this).stringof);
  }

  /**
     If this is a record, enum or fixed, returns its namespace-qualified name,
     otherwise returns the name of the primitive type.
  */
  public string getFullname() {
    return getName();
  }

  /// If this is a record, enum, or fixed, add an alias.
  public void addAlias(string name, string namespace = null) {
    throw new AvroRuntimeException("Not a named type: " ~ this.toString);
  }

  /// If this is a record, enum, or fixed, return its aliases, if any.
  public bool[string] getAliases() {
    throw new AvroRuntimeException("Not a named type: " ~ this.toString);
  }

  /// Indicates whether the schema is a both a record an an error type in a protocol.
  public bool isError() {
    throw new AvroRuntimeException("Not a record: " ~ this.toString);
  }

  /// If this is an array, returns its element type.
  public const(Schema) getElementSchema() const {
    throw new AvroRuntimeException("Not an array: " ~ this.toString);
  }

  /// If this is a map, returns its value type.
  public const(Schema) getValueSchema() const {
    throw new AvroRuntimeException("Not a map: " ~ this.toString);
  }

  /// If this is a union, returns its types.
  public const(Schema[]) getTypes() const {
    throw new AvroRuntimeException("Not a union: " ~ this.toString);
  }

  /// If this is a union, return the branch with the provided full name.
  public size_t getIndexNamed(string name) {
    throw new AvroRuntimeException("Not a union: " ~ this.toString);
  }

  /// If this is fixed, returns its size.
  public size_t getFixedSize() const {
    throw new AvroRuntimeException("Not fixed: " ~ this.toString);
  }

  override
  public string toString() const {
    return typeid(typeof(this)).stringof;
  }
}

package class NullSchema : Schema {
  this() {
    super(Type.NULL);
  }
}

unittest {
  auto schema = new NullSchema();
  assert(schema.getType() == Type.NULL);
  assert(schema.getName() == "null");
  assert(schema.getFullname() == "null");
  assertThrown!AvroRuntimeException(schema.getField("a"));
}

package class BooleanSchema : Schema {
  this() {
    super(Type.BOOLEAN);
  }
}

unittest {
  auto schema = new BooleanSchema();
  assert(schema.getType() == Type.BOOLEAN);
  assert(schema.getName() == "boolean");
  assert(schema.getFullname() == "boolean");
  assertThrown!AvroRuntimeException(schema.getField("a"));
}

package class IntSchema : Schema {
  this() {
    super(Type.INT);
  }
}

unittest {
  auto schema = new IntSchema();
  assert(schema.getType() == Type.INT);
  assert(schema.getName() == "int");
  assert(schema.getFullname() == "int");
  assertThrown!AvroRuntimeException(schema.getField("a"));
}

package class LongSchema : Schema {
  this() {
    super(Type.LONG);
  }
}

unittest {
  auto schema = new IntSchema();
  assert(schema.getType() == Type.INT);
  assert(schema.getName() == "int");
  assert(schema.getFullname() == "int");
  assertThrown!AvroRuntimeException(schema.getField("a"));
}

package class FloatSchema : Schema {
  this() {
    super(Type.FLOAT);
  }
}

unittest {
  auto schema = new FloatSchema();
  assert(schema.getType() == Type.FLOAT);
  assert(schema.getName() == "float");
  assert(schema.getFullname() == "float");
  assertThrown!AvroRuntimeException(schema.getField("a"));
}

package class DoubleSchema : Schema {
  this() {
    super(Type.DOUBLE);
  }
}

unittest {
  auto schema = new DoubleSchema();
  assert(schema.getType() == Type.DOUBLE);
  assert(schema.getName() == "double");
  assert(schema.getFullname() == "double");
  assertThrown!AvroRuntimeException(schema.getField("a"));
}

package class BytesSchema : Schema {
  this() {
    super(Type.BYTES);
  }
}

unittest {
  auto schema = new BytesSchema();
  assert(schema.getType() == Type.BYTES);
  assert(schema.getName() == "bytes");
  assert(schema.getFullname() == "bytes");
  assertThrown!AvroRuntimeException(schema.getField("a"));
}

package class StringSchema : Schema {
  this() {
    super(Type.STRING);
  }
}

unittest {
  auto schema = new StringSchema();
  assert(schema.getType() == Type.STRING);
  assert(schema.getName() == "string");
  assert(schema.getFullname() == "string");
  assertThrown!AvroRuntimeException(schema.getField("a"));
}

/// Only certain schema types, like `record`, `enum`, and `fixed` have names and aliases.
package abstract class NamedSchema : Schema {
  /**
     A full name (including namespace) of the schema.
     See_Also: https://avro.apache.org/docs/current/spec.html#names
  */
  Name name;

  /// A description of the field for users.
  string doc;

  /**
     Aliases used during deserialization for a field (commonly used for schema evolution).
     See_Also: https://avro.apache.org/docs/current/spec.html#Aliases
   */
  bool[Name] aliases;  // Use a hash-set for faster lookup.

  public this(Type type, Name name, string doc) {
    super(type);
    this.name = name;
    this.doc = doc;
    if (name.fullname in PRIMITIVE_TYPE_BY_NAME) {
      throw new AvroTypeException("Schemas may not be named after primitives: " ~ name.fullname);
    }
  }

  override
  public string getName() {
    return name.name;
  }

  override
  public string getDoc() {
    return doc;
  }

  override
  public string getNamespace() {
    return name.namespace;
  }

  override
  public string getFullname() {
    return name.fullname;
  }

  override
  public void addAlias(string name, string namespace = null) {
    if (namespace == null)
      namespace = this.name.namespace;
    aliases[new Name(name, namespace)] = true;
  }
}

/**
   A named type that contains a set of fields, each of which may have a different type.

   Records are similar to JSON Objects or Protocol Buffer messages.

   See_Also: https://avro.apache.org/docs/current/spec.html#schema_record
*/
package class RecordSchema : NamedSchema {
  /// A list of contained fields including a name, type-schema, and default value.
  private Field[] _fields;
  private Field[string] _fieldMap;
  private bool _isError;

  this(Name name, string doc, bool isError) {
    super(Type.RECORD, name, doc);
    this._isError = isError;
  }

  this(Name name, string doc, bool isError, Field[] fields) {
    this(name, doc, isError);
    setFields(fields);
  }

  override
  public const(Field) getField(string fieldname) const {
    return _fieldMap.get(fieldname, null);
  }

  override
  public const(Field[]) getFields() const {
    return _fields;
  }

  override
  public void setFields(Field[] fields) {
    import std.format : format;
    if (_fields.length > 0) {
      throw new AvroRuntimeException("Fields are already set");
    }
    int i = 0;
    foreach (Field f; fields) {
      if (f.position != -1) {
        throw new AvroRuntimeException("Field already used: " ~ f.toString);
      }
      f.position = i++;
      if (f.name in _fieldMap) {
        throw new AvroRuntimeException(
            format("Duplicate field %s in record %s: %s and %s.",
                f.name, name, f, _fieldMap[f.name]));
      }
      _fieldMap[f.name] = f;
      _fields ~= f;
    }
  }

  override
  public bool isError() {
    return _isError;
  }
}

unittest {
  auto schema = new RecordSchema(
      new Name("fish", "com.example"),
      "hello-doc",
      false,
      [
          new Field("a", new IntSchema(), "", JSONValue(3), true, Field.Order.IGNORE),
          new Field("b", new StringSchema(), "", JSONValue("ab"), true, Field.Order.IGNORE)
      ]);
  assert(schema.getType() == Type.RECORD);
  assert(schema.getName() == "fish");
  assert(schema.getFullname() == "com.example.fish");
  assert(schema.isError() == false);
  assert(schema.getFields().length == 2);
  assert(schema.getField("a").getName() == "a");
  assert(schema.getField("a").getPosition() == 0);
  assert(schema.getField("b").getName() == "b");
  assert(schema.getField("b").getPosition() == 1);
}


/**
   A schema that represents an enumerated list of specific valid values.
   See_Also: https://avro.apache.org/docs/current/spec.html#Enums
 */
package class EnumSchema : NamedSchema {
  private string[] symbols;
  private int[string] ordinals;
  private string enumDefault;

  this(Name name, string doc, string[] symbols, string enumDefault) {
    import std.algorithm : canFind;

    super(Type.ENUM, name, doc);
    this.symbols = symbols;
    this.enumDefault = enumDefault;
    int i = 0;
    foreach (string symbol; symbols) {
      if (symbol in ordinals) {
        throw new SchemaParseException("Duplicate enum symbol: " ~ symbol);
      }
      ordinals[Name.validateName(symbol)] = i++;
    }
    if (enumDefault !is null && !symbols.canFind(enumDefault)) {
      throw new SchemaParseException(
          "The Enum Default: " ~ enumDefault ~ " is not in the enum symbol set: "
          ~ symbols.to!string);
    }
    ordinals.rehash;
  }

  override
  public const(string[]) getEnumSymbols() const {
    return symbols;
  }

  override
  public bool hasEnumSymbol(string symbol) const {
    return (symbol in ordinals) != null;
  }

  override
  public size_t getEnumOrdinal(string symbol) const {
    return ordinals[symbol];
  }

  override
  public string getEnumDefault() {
    return enumDefault;
  }
}

unittest {
  auto schema = new EnumSchema(
      new Name("employment", "com.example"), "ham", ["PART_TIME", "FULL_TIME"], "FULL_TIME");
  assert(schema.getEnumSymbols() == ["PART_TIME", "FULL_TIME"]);
  assert(schema.hasEnumSymbol("PART_TIME") == true);
  assert(schema.hasEnumSymbol("QUASI_TIME") == false);
  assert(schema.getEnumOrdinal("PART_TIME") == 0);
  assert(schema.getEnumOrdinal("FULL_TIME") == 1);
  assert(schema.getEnumDefault() == "FULL_TIME");
}

unittest {
  // Invalid default.
  assertThrown!SchemaParseException(new EnumSchema(
      new Name("employment", "com.example"), "ham", ["PART_TIME", "FULL_TIME"], "QUASI_TIME"));
  // Duplicate symbol.
  assertThrown!SchemaParseException(new EnumSchema(
      new Name("employment", "com.example"), "ham", ["PART_TIME", "PART_TIME"], "PART_TIME"));
}

/**
   A list of items that all share the same schema type.
   See_Also: https://avro.apache.org/docs/current/spec.html#Arrays
*/
package class ArraySchema : Schema {
  private Schema elementType;

  this(Schema elementType) {
    super(Type.ARRAY);
    this.elementType = elementType;
  }

  override
  public const(Schema) getElementSchema() const {
    return elementType;
  }
}

unittest {
  auto schema = new ArraySchema(Schema.createPrimitive(Type.INT));
  assert(schema.getType() == Type.ARRAY);
  assert(schema.getName() == "array");
  assert(schema.getFullname() == "array");
  assert(schema.getElementSchema().getType() == Type.INT);
}

/**
   An associative array mapping a string key to a value of a given schema type.
   See_Also: https://avro.apache.org/docs/current/spec.html#Maps
*/
package class MapSchema : Schema {
  private Schema valueType;

  this(Schema valueType) {
    super(Type.MAP);
    this.valueType = valueType;
  }

  override
  public const(Schema) getValueSchema() const {
    return valueType;
  }
}

unittest {
  auto schema = new MapSchema(Schema.createPrimitive(Type.INT));
  assert(schema.getType() == Type.MAP);
  assert(schema.getName() == "map");
  assert(schema.getFullname() == "map");
  assert(schema.getValueSchema().getType() == Type.INT);
}

/**
  A schema that represents data that can have one type among a list of several schemas.
  See_Also: https://avro.apache.org/docs/current/spec.html#Unions
*/
package class UnionSchema : Schema {
  private Schema[] types;
  private int[string] indexByName;

  this(Schema[] types) {
    super(Type.UNION);
    this.types = types;
    int index = 0;
    foreach (Schema type; types) {
      if (type.getType() == Type.UNION) {
        throw new AvroRuntimeException("Nested union: " ~ this.toString);
      }
      string name = type.getFullname();
      if (name is null) {
        throw new AvroRuntimeException("Nameless in union: " ~ this.toString);
      }
      if (name in indexByName) {
        throw new AvroRuntimeException("Duplicate in union: " ~ name);
      }
      indexByName[name] = index++;
    }
    indexByName.rehash;
  }

  override
  public const(Schema[]) getTypes() const {
    return types;
  }

  override
  public size_t getIndexNamed(string name) {
    return indexByName[name];
  }
}

unittest {
  auto schema = new UnionSchema([
      Schema.createPrimitive(Type.STRING),
      Schema.createPrimitive(Type.INT)]);
  assert(schema.getType() == Type.UNION);
  assert(schema.getName() == "union");
  assert(schema.getFullname() == "union");
  assert(schema.getTypes().length == 2);
  assert(schema.getIndexNamed("string") == 0);
  assert(schema.getIndexNamed("int") == 1);
}

package class FixedSchema : NamedSchema {
  private size_t size;

  this(Name name, string doc, size_t size) {
    super(Type.FIXED, name, doc);
    this.size = size;
  }

  override
  public size_t getFixedSize() const {
    return size;
  }
}

unittest {
  auto schema = new FixedSchema(new Name("bob", "com.example"), "fixed doc", 10);
  assert(schema.getType() == Type.FIXED);
  assert(schema.getName() == "bob");
  assert(schema.getFullname() == "com.example.bob");
  assert(schema.getFixedSize() == 10);
}
