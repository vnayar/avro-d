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
  /// Within a record, the relative order of a field.
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
        ? Schema.validateDefault(name, schema, defaultValue) : defaultValue;
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

  public string getName() {
    return name;
  }

  public int getPosition() {
    return position;
  }

  public Schema getSchema() {
    return schema;
  }

  public string getDoc() {
    return doc;
  }

  public bool hasDefaultValue() {
    return schema.getType() == Type.NULL || !defaultValue.isNull();
  }

  public JSONValue getDefaultValue() {
    return defaultValue;
  }

  public Order getOrder() {
    return order;
  }

  public void addAlias(string name) {
    aliases[name] = true;
  }

  public bool[string] getAliases() {
    return aliases;
  }
}
