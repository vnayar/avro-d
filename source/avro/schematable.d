/// A table of observed schemas organized by name, used during schema parsing.
module avro.schematable;

import std.typecons : Rebindable, rebindable;

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

   Params:
     SchemaT = Allows the caller to override the stored type, e.g. with a const.

   See_Also: https://avro.apache.org/docs/current/spec.html#names
*/
class SchemaTable(SchemaT : const(Schema) = Schema) {
  /// Maps a fully qualified name to a Schema.
  private Rebindable!(SchemaT)[string] _schemaByName;

  /// The namespace that should be used as the default during schema parsing.
  private string _defaultNamespace;

  public string defaultNamespace() const {
    return _defaultNamespace;
  }

  public string defaultNamespace(string defaultNamespace) {
    return this._defaultNamespace = defaultNamespace;
  }

  public SchemaT getSchemaByName(const Name name) {
    if (name.getFullname() in _schemaByName) {
      return _schemaByName[name.getFullname()];
    } else {
      return null;
    }
  }

  /// Look up a previously known Schema by its name.
  public SchemaT getSchemaByName(string name) {
    if (name in PRIMITIVE_TYPE_BY_NAME) {
      return Schema.createPrimitive(PRIMITIVE_TYPE_BY_NAME[name]);
    }
    Name schemaName = new Name(name, _defaultNamespace);
    return getSchemaByName(schemaName);
  }

  /// Indicates whether a given name is known in the SchemaTable.
  public bool containsName(const Name name) const {
    return (name.getFullname() in _schemaByName) != null;
  }

  /// Adds a new schema to the set of known schemas.
  public void addSchema(SchemaT schema) {
    import avro.schema : NamedSchema;
    _schemaByName[schema.getFullname()] = rebindable(schema);
  }
}

