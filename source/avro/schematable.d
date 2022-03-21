/// A table of observed schemas organized by name, used during schema parsing.
module avro.schematable;

import avro.type : PRIMITIVE_TYPE_BY_NAME;
import avro.schema : Schema;
import avro.name : Name;

@safe:

/**
   A lookup table of known names for Schemas and the default namespace used during parsing.

   Some names are known before the schema is read, such as primitive types, which have no
   namespace and whose name matches their type. During parsing, if a schema does not
   designate a namespace, it inherits the namespace from the most tightly enclosing schema
   or protocol.

   See_Also: https://avro.apache.org/docs/current/spec.html#names
*/
class SchemaTable {
  /// Maps a fully qualified name to a Schema.
  private Schema[Name] _schemaByName;

  /// The namespace that should be used as the default during schema parsing.
  private string _defaultNamespace;

  public string defaultNamespace() {
    return _defaultNamespace;
  }

  public string defaultNamespace(string defaultNamespace) {
    return this._defaultNamespace = defaultNamespace;
  }

  public Schema getSchemaByName(Name name) {
    if (name in _schemaByName) {
      return _schemaByName[name];
    } else {
      return null;
    }
  }

  /// Look up a previously known Schema by its name.
  public Schema getSchemaByName(string name) {
    if (name in PRIMITIVE_TYPE_BY_NAME) {
      return Schema.createPrimitive(PRIMITIVE_TYPE_BY_NAME[name]);
    }
    Name schemaName = new Name(name, _defaultNamespace);
    return getSchemaByName(schemaName);
  }

  /// Indicates whether a given name is known in the SchemaTable.
  public bool containsName(Name name) {
    return (name in _schemaByName) != null;
  }

  /// Adds a new schema to the set of known schemas.
  public void addSchema(Schema schema) {
    import avro.schema : NamedSchema;
    _schemaByName[(cast(NamedSchema) schema).name] = schema;
  }
}

