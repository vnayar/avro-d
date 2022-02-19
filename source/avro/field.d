module avro.field;

import std.json : JSONValue, JSONType;

import avro.type : Type;
import avro.name : Name;
import avro.schema : Schema;
import avro.attributes : HasJsonAttributes;
import avro.exception : AvroTypeException;

/**
   A field within a record.

   See_Also: https://avro.apache.org/docs/current/spec.html#schema_record
*/
public class Field {
  public static JSONValue validateDefault(string fieldName, Schema schema, JSONValue defaultValue) {
    if (!isValidDefault(schema, defaultValue)) {
      string message = "Invalid default for field " ~ fieldName ~ ": " ~ defaultValue.toString
          ~ " not a " ~ schema.toString;
      throw new AvroTypeException(message);
    }
    return defaultValue;
  }

  unittest {
    import std.exception : assertThrown, assertNotThrown;
    import std.json : parseJSON;
    import avro.schema;

    // Valid string defaults.
    auto stringSchema = Schema.createPrimitive(Type.STRING);
    assertNotThrown(validateDefault("a", stringSchema, JSONValue("fish")));
    assertThrown!AvroTypeException(validateDefault("a", stringSchema, JSONValue(3)));
    assertThrown!AvroTypeException(validateDefault("a", stringSchema, JSONValue(null)));


    // Valid integer defaults.
    auto intSchema = Schema.createPrimitive(Type.INT);
    assertNotThrown(validateDefault("b", intSchema, JSONValue(1234)));
    assertThrown!AvroTypeException(validateDefault("b", intSchema, JSONValue(4294967296)));
    assertThrown!AvroTypeException(validateDefault("b", intSchema, JSONValue("bear")));

    // Valid long defaults
    auto longSchema = Schema.createPrimitive(Type.LONG);
    assertNotThrown(validateDefault("c", longSchema, JSONValue(4294967296)));
    assertThrown!AvroTypeException(validateDefault("c", longSchema, JSONValue(null)));
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
            new Field("a", intSchema, "", JSONValue(3), true, Order.IGNORE),
            new Field("b", stringSchema, "", JSONValue("ab"), true, Order.IGNORE)
        ]);
    assertNotThrown(validateDefault("i", recordSchema, parseJSON(`{"a": 3, "b": "ab"}`)));
    assertThrown!AvroTypeException(
        validateDefault("i", recordSchema, JSONValue(["a": "ab", "b": "ab"])));
    assertThrown!AvroTypeException(validateDefault("i", recordSchema, JSONValue(3)));

  }

  private static bool isValidDefault(Schema schema, JSONValue defaultValue) {
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
          if (!isValidDefault(schema.getElementType(), element))
            return false;
        return true;
      case Type.MAP:
        if (defaultValue.type != JSONType.object)
          return false;
        foreach (JSONValue value; defaultValue.object)
          if (!isValidDefault(schema.getValueType(), value))
            return false;
        return true;
      case Type.UNION: // union default: first branch
        return isValidDefault(schema.getTypes()[0], defaultValue);
      case Type.RECORD:
        if (defaultValue.type != JSONType.object)
          return false;
        foreach (Field field; schema.getFields()) {
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

  /**
     Objects with identical schemas may be sorted by a depth-first left-to-right traversal of the
     schema.

     See_Also: https://avro.apache.org/docs/current/spec.html#order
  */
  public enum Order {
    /// The default sort order for fields.
    ASCENDING,
    /// The order of fields is reversed.
    DESCENDING,
    /// Ignore this value for the purpose of sorting.
    IGNORE
  }

  /// A required name for the field.
  package string name;
  package int position = -1;
  package Schema schema;
  /// An optional description of the field for users.
  package string doc;
  /// A default value to use when deserializing data encoded from a schema that lacks the field.
  package JSONValue defaultValue;
  /// Describe how the field influences record sorting.
  package Order order;
  /// A list of strings providing alternate names for this field (for schema migration).
  package bool[string] aliases;

  mixin HasJsonAttributes;

  this(
      string name, Schema schema, string doc, JSONValue defaultValue,
      bool shouldValidateDefault, Order order) {
    this.name = Name.validateName(name);
    this.schema = schema;
    this.doc = doc;
    this.defaultValue = shouldValidateDefault
        ? validateDefault(name, schema, defaultValue) : defaultValue;
    this.order = order;
  }

  /**
     Constructs a new Field instance with the same [name], [doc],
     [defaultValue], and [order] as [field] has with changing
     the schema to the specified one. It also copies all the [attributes] and
     [aliases].
   */
  this(Field field, Schema schema) {
    this(field.name, schema, field.doc, field.defaultValue, true, field.order);
    foreach (string attrKey, JSONValue attrValue; field.getAttributes()) {
      attributes[attrKey] = attrValue;
    }
  }
}
