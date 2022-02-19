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

  /// Return the type of this schema.
  public Type getType() {
    return type;
  }

  /**
     Return the logical type, which can be combined with a type for special interpretation,
     like a timestamp or a date.
  */
  public string getLogicalType() {
    return logicalType;
  }

  /**
    If this is a record, returns the Field with the given name [fieldName]. If there is no field by
    that name, a [null] is returned.
  */
  public Field getField(string fieldName) {
    throw new AvroRuntimeException("Not a record: " ~ this.toString);
  }

  /**
    If this is a record, returns the fields in it. The returned list is in the order of their
    positions.
  */
  public Field[] getFields() {
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
  public string[] getEnumSymbols() {
    throw new AvroRuntimeException("Not an enum: " ~ this.toString);
  }

  /// If this is an enum, return its default value.
  public string getEnumDefault() {
    throw new AvroRuntimeException("Not an enum: " ~ this.toString);
  }

  /// If this is an enum, return a symbol's ordinal value.
  public int getEnumOrdinal(string symbol) {
    throw new AvroRuntimeException("Not an enum: " ~ this.toString);
  }

  /// If this is an enum, returns true if it contains given symbol.
  public bool hasEnumSymbol(string symbol) {
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
  public Schema getElementType() {
    throw new AvroRuntimeException("Not an array: " ~ this.toString);
  }

  /// If this is a map, returns its value type.
  public Schema getValueType() {
    throw new AvroRuntimeException("Not a map: " ~ this.toString);
  }

  /// If this is a union, returns its types.
  public Schema[] getTypes() {
    throw new AvroRuntimeException("Not a union: " ~ this.toString);
  }

  /// If this is fixed, returns its size.
  public size_t getFixedSize() {
    throw new AvroRuntimeException("Not fixed: " ~ this.toString);
  }

}

package class NullSchema : Schema {
  this() {
    super(Type.NULL);
  }
}

package class BooleanSchema : Schema {
  this() {
    super(Type.BOOLEAN);
  }
}

package class IntSchema : Schema {
  this() {
    super(Type.INT);
  }
}

package class LongSchema : Schema {
  this() {
    super(Type.LONG);
  }
}

package class FloatSchema : Schema {
  this() {
    super(Type.FLOAT);
  }
}

package class DoubleSchema : Schema {
  this() {
    super(Type.DOUBLE);
  }
}

package class BytesSchema : Schema {
  this() {
    super(Type.BYTES);
  }
}

package class StringSchema : Schema {
  this() {
    super(Type.STRING);
  }
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
  public Field getField(string fieldname) {
    return _fieldMap.get(fieldname, null);
  }

  override
  public Field[] getFields() {
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
  public string[] getEnumSymbols() {
    return symbols;
  }

  override
  public bool hasEnumSymbol(string symbol) {
    return (symbol in ordinals) != null;
  }

  override
  public int getEnumOrdinal(string symbol) {
    return ordinals[symbol];
  }

  override
  public string getEnumDefault() {
    return enumDefault;
  }
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
  public Schema getElementType() {
    return elementType;
  }
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
  public Schema getValueType() {
    return valueType;
  }
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
  public Schema[] getTypes() {
    return types;
  }
}

package class FixedSchema : NamedSchema {
  private size_t size;

  this(Name name, string doc, size_t size) {
    super(Type.FIXED, name, doc);
    this.size = size;
  }

  override
  public size_t getFixedSize() {
    return size;
  }
}
