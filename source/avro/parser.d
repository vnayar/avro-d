module avro.parser;

import std.conv : to;
import std.json : JSONValue, JSONType, parseJSON;
import std.stdio : stderr;

import avro.schema;
import avro.field : Field;
import avro.name : Name;
import avro.type : Type, PRIMITIVE_TYPE_BY_NAME;
import avro.schematable : SchemaTable;
import avro.attributes : HasJsonAttributes;
import avro.orderedmap : OrderedMap;
import avro.exception : AvroRuntimeException, AvroTypeException, SchemaParseException;

/**
   A parser for JSON-format schemas. Eached named schema parsed with a parser is added to the names
   known to the parser so that subsequently parsed schemas may refer to it by name.
*/
class Parser {
  // A set of attribute names that are reserved by the Avro specification.
  public static enum bool[string] RESERVED_ATTRIBUTES = [
      "doc": true, "fields": true, "items": true, "name": true, "namespace": true,
      "size": true, "symbols": true, "values": true, "type": true, "aliases": true,
      "default": true
  ];

  private SchemaTable schemaTable = new SchemaTable();

  /// Adds the provided types to the set of defined and named types known to this parser.
  public Parser addSchemas(Schema[string] types) {
    foreach (Schema s; types.values)
      schemaTable.addSchema(s);
    return this;
  }

  /// Builds a [Schema] using a path to a ".avsc" file.
  public Schema parseFile(string fileName) {
    import std.file : readText;
    string text = readText(fileName);
    return parseText(text);
  }

  /// Builds a [Schema] from JSON text.
  public Schema parseText(string text) {
    import std.json : parseJSON;
    JSONValue json = parseJSON(text);
    return parseJson(json);
  }

  unittest {
    import std.stdio;
    auto parser = new Parser();
    Schema schema =  parser.parseText(q"EOS
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number", "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}
EOS");
    assert(schema.getType() == Type.RECORD);
    Field[] fields = schema.getFields();
    assert(schema.getName() == "User");
    assert(schema.getNamespace() == "example.avro");
    assert(schema.getFullname() == "example.avro.User");
    assert(fields.length == 3);
    assert(fields[0].getName() == "name");
    assert(fields[0].getSchema().getType() == Type.STRING);
    assert(fields[1].getName() == "favorite_number");
    assert(fields[1].getSchema().getType() == Type.UNION);
    assert(fields[2].getName() == "favorite_color");
  }

  /// Builds a [Schema] from a JSON parse tree.
  public Schema parseJson(JSONValue jsonSchema) {
    import std.algorithm : map;
    import std.uni : toUpper;

    // Form 1 of a schema, a string naming a type.
    if (jsonSchema.type == JSONType.string) {
      Schema result = schemaTable.getSchemaByName(jsonSchema.str());
      if (result is null) {
        throw new SchemaParseException("Undefined name: " ~ jsonSchema.toString);
      }
      return result;
    } else if (jsonSchema.type == JSONType.object) {
      Schema result;
      string type = getRequiredText(jsonSchema, "type", "No type");
      Name name = null;
      string savedNamespace = schemaTable.defaultNamespace();
      string doc = null;
      bool isTypeError = (type == "error");
      bool isTypeRecord = (type == "record");
      bool isTypeEnum = (type == "enum");
      bool isTypeFixed = (type == "fixed");
      if (isTypeRecord || isTypeError || isTypeEnum || isTypeFixed) {
        string namespace = getOptionalText(jsonSchema, "namespace");
        doc = getOptionalText(jsonSchema, "doc");
        if (namespace == null)
          namespace = savedNamespace;
        name = new Name(getRequiredText(jsonSchema, "name", "No name in schema"), namespace);
        schemaTable.defaultNamespace(name.namespace); // set default namespace
      }
      if (type in PRIMITIVE_TYPE_BY_NAME) { // primitive
        result = Schema.createPrimitive(PRIMITIVE_TYPE_BY_NAME[type]);
      } else if (isTypeRecord || isTypeError) { // record
        result = new RecordSchema(name, doc, isTypeError);
        if (name !is null)
          schemaTable.addSchema(result);

        if ("fields" !in jsonSchema || jsonSchema["fields"].type != JSONType.array)
          throw new SchemaParseException("Record has no fields: " ~ jsonSchema.toString);
        JSONValue fieldsNode = jsonSchema["fields"];
        Field[] fields;
        fields.reserve(fieldsNode.array.length);
        foreach (JSONValue jsonField; fieldsNode.array) {
          string fieldName = getRequiredText(jsonField, "name", "No field name");
          string fieldDoc = getOptionalText(jsonField, "doc");
          if ("type" !in jsonField)
            throw new SchemaParseException("No field type: " ~ jsonField.toString);
          if (jsonField["type"].type == JSONType.string
              && schemaTable.getSchemaByName(jsonField["type"].str) is null) {
            throw new SchemaParseException(
                jsonField["type"].toString ~ " is not a defined name. "
                ~ "The type of the \"" ~ fieldName ~ "\" field must be defined or "
                ~ "a {\"type\": ... } expression.");
          }
          JSONValue fieldTypeNode = jsonField["type"];
          Schema fieldTypeSchema = parseJson(fieldTypeNode);
          Field.Order order = Field.Order.ASCENDING;
          if ("order" in jsonField)
            order = jsonField["order"].str.toUpper.to!(Field.Order);
          JSONValue jsonFieldDefault = "default" in jsonField
              ? jsonField["default"] : JSONValue(null);
          if (!jsonFieldDefault.isNull()
              && (fieldTypeSchema.getType() == Type.FLOAT || fieldTypeSchema.getType() == Type.DOUBLE)
              && jsonFieldDefault.type == JSONType.string) {
            jsonFieldDefault = JSONValue(jsonFieldDefault.str.to!double);
          }
          Field f = new Field(fieldName, fieldTypeSchema, fieldDoc, jsonFieldDefault, true, order);
          foreach (string fieldAttrKey, JSONValue fieldAttrJson; jsonField.object) {
            if (fieldAttrKey !in RESERVED_ATTRIBUTES)
              f.addAttribute(fieldAttrKey, jsonField.object[fieldAttrKey]);
          }
          f.aliases = parseAliases(jsonField);
          fields ~= f;
          if (fieldTypeSchema.getLogicalType() is null
              && getOptionalText(jsonField, "logicalType") !is null) {
            stderr.writefln(
                "WARN: Ignored the %s.%s.logicalType property (\"%s\"). It should probably "
                ~ "be nested inside the \"type\" for the field.",
                name, fieldName, getOptionalText(jsonField, "logicalType"));
          }
        }
        result.setFields(fields);
      } else if (isTypeEnum) { // enum
        JSONValue symbolsNode = "symbols" in jsonSchema ? jsonSchema["symbols"] : JSONValue(null);
        if (symbolsNode.isNull || symbolsNode.type != JSONType.array)
          throw new SchemaParseException("Enum has no symbols: " ~ jsonSchema.toString);
        string[] symbols;
        symbols.reserve(symbolsNode.array.length);
        foreach (JSONValue n; symbolsNode.array)
          symbols ~= n.str;
        JSONValue enumDefaultNode = "default" in jsonSchema ? jsonSchema["default"] : JSONValue(null);
        string defaultSymbol = null;
        if (!enumDefaultNode.isNull)
          defaultSymbol = enumDefaultNode.str;
        result = new EnumSchema(name, doc, symbols, defaultSymbol);
        if (name !is null)
          schemaTable.addSchema(result);
      } else if (type == "array") { // array
        JSONValue itemsNode = "items" in jsonSchema ? jsonSchema["items"] : JSONValue(null);
        if (itemsNode.isNull)
          throw new SchemaParseException("Array has no items type: " ~ jsonSchema.toString);
        result = new ArraySchema(parseJson(itemsNode));
      } else if (type == "map") { // map
        JSONValue valuesNode = "values" in jsonSchema ? jsonSchema["values"] : JSONValue(null);
        if (valuesNode.isNull)
          throw new SchemaParseException("Map has no values type: " ~ jsonSchema.toString);
        result = new MapSchema(parseJson(valuesNode));
      } else if (isTypeFixed) { // fixed
        JSONValue sizeNode = "size" in jsonSchema ? jsonSchema["size"] : JSONValue(null);
        if (sizeNode.isNull || sizeNode.type != JSONType.integer)
          throw new SchemaParseException("invalid or no size: " ~ jsonSchema.toString);
        result = new FixedSchema(name, doc, sizeNode.uinteger);
        if (name !is null)
          schemaTable.addSchema(result);
      } else { // for unions with self reference
        Name nameFromType = new Name(type, schemaTable.defaultNamespace);
        if (schemaTable.containsName(nameFromType)) {
          return schemaTable.getSchemaByName(nameFromType);
        }
        throw new SchemaParseException("Type not supported: " ~ type);
      }

      // Now process the the custom attributes for the schema.
      foreach (string attrKey, JSONValue attrJson; jsonSchema.object) {
        if (attrKey !in RESERVED_ATTRIBUTES) // ignore reserved
          result.addAttribute(attrKey, attrJson);
      }

      // parse logical type if present
      // TODO: Add logic to understand logical types.
      result.logicalType = null;
      schemaTable.defaultNamespace(savedNamespace); // restore space
      if (typeid(result) == NamedSchema.classinfo) {
        bool[string] aliases = parseAliases(jsonSchema);
        foreach (string alias_; aliases.keys) {
          result.addAlias(alias_);
        }
      }
      return result;
    } else if (jsonSchema.type == JSONType.array) { // union
      Schema[] types;
      types.reserve(jsonSchema.array.length);
      foreach (JSONValue typeNode; jsonSchema.array)
        types ~= parseJson(typeNode);
      return new UnionSchema(types);
    } else {
      throw new SchemaParseException("Schema not yet supported: " ~ jsonSchema.toString);
    }
  }

  /**
     Extracts text value associated with a key from an object JSON node, and throws
     [SchemaParseException] if it doesn't exist.

     Params:
     container = JSON node which has the text attribute.
     key       = The name of the JSON attribute that has the text value.
     error     = The string to prepend to the SchemaParseException message.
  */
  private static string getRequiredText(JSONValue container, string key, string error) {
    string text = getOptionalText(container, key);
    if (text is null) {
      throw new SchemaParseException(error ~ ": " ~ container.toString);
    }
    return text;
  }

  /// Extracts a text value by key from a JSON node.
  private static string getOptionalText(JSONValue container, string key) {
    if (container.type == JSONType.object && key in container) {
      JSONValue jsonValue = container[key];
      if (jsonValue.type == JSONType.string)
        return jsonValue.str;
    }
    return null;
  }

  /// Extracts and validates the "aliases" field for a schema.
  static bool[string] parseAliases(JSONValue node) {
    bool[string] aliases;
    if ("aliases" !in node)
      return aliases;
    JSONValue aliasesNode = node["aliases"];
    if (aliasesNode.type != JSONType.array)
      throw new SchemaParseException("aliases not an array: " ~ node.toString);
    foreach (JSONValue aliasNode; aliasesNode.array) {
      if (aliasNode.type != JSONType.string)
        throw new SchemaParseException("alias not a string: " ~ aliasNode.toString);
      aliases[aliasNode.str] = true;
    }
    return aliases;
  }
}
