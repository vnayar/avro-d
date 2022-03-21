/// Avro schemas are often identified by names, which are separated via namespaces.
module avro.name;

import avro.exception : SchemaParseException;

@safe:

/**
   A fully qualified schema name, which includes a namespace and an individual name.

   See_Also: https://avro.apache.org/docs/current/spec.html#names
*/
class Name {

  /**
     The name portion of a fullname, record field names, and enum symbols must:
     - start with \[A-Za-z_\]
     - subsequently contain only \[A-Za-z0-9_\]

     See_Also: https://avro.apache.org/docs/current/spec.html#names
  */
  public static string validateName(string name) {
    import std.uni : isAlpha, isAlphaNum;
    if (name is null)
      throw new SchemaParseException("Null name");
    size_t length = name.length;
    if (length == 0)
      throw new SchemaParseException("Empty name");
    char first = name[0];
    if (!(first.isAlpha() || first == '_'))
      throw new SchemaParseException("Illegal initial character: " ~ name);
    for (int i = 1; i < length; i++) {
      char c = name[i];
      if (!(c.isAlphaNum() || c == '_'))
        throw new SchemaParseException("Illegal character in: " ~ name);
    }
    return name;
  }

  ///
  unittest {
    import std.exception : assertThrown, assertNotThrown;
    assertThrown!SchemaParseException(validateName(null));
    assertThrown!SchemaParseException(validateName(""));
    assertThrown!SchemaParseException(validateName("3a"));
    assertThrown!SchemaParseException(validateName("h@t"));
    assert(validateName("hat3") == "hat3");
    assert(validateName("_fish") == "_fish");
  }

  package string name;
  package string namespace;
  package string fullname;

  /// Creates a new Name using the provided namespace if the name does not have one.
  this(string name, string namespace) {
    import std.string : lastIndexOf;

    // Anonymous schema?
    if (name == null) {
      this.name = this.namespace = this.fullname = null;
      return;
    }

    // Does the name contain the namespace?
    long lastDot = name.lastIndexOf('.');
    if (lastDot < 0) { // namespace absent
      validateName(name);
      this.name = name;
    } else { // namespace present
      namespace = name[0 .. lastDot];
      this.name = validateName(name[lastDot + 1 .. $]);
    }
    if (namespace == "")
      namespace = null;
    this.namespace = namespace;
    this.fullname = (this.namespace == null) ? this.name : this.namespace ~ "." ~ this.name;
  }

  ///
  unittest {
    Name name1 = new Name("bob", null);
    assert(name1.name == "bob" && name1.namespace is null && name1.fullname == "bob");
    Name name2 = new Name("bob", "");
    assert(name2.name == "bob" && name2.namespace is null && name2.fullname == "bob");
    Name name3 = new Name("bob", "com.example");
    assert(name3.name == "bob" && name3.namespace == "com.example"
        && name3.fullname == "com.example.bob");
    // If the name contains a namespace already, the provided one will be ignored.
    Name name4 = new Name("com.example.bob", "org.funny");
    assert(name4.name == "bob" && name4.namespace == "com.example"
        && name4.fullname == "com.example.bob");

    // Without a name, the namespace is ignored.
    Name name5 = new Name(null, "com.example");
    assert(name5.name is null && name5.namespace is null && name5.fullname is null);
  }

  /// The name of a schema without its namespace.
  string getName() const {
    return name;
  }

  /// The namespace of a schema.
  string getNamespace() const {
    return namespace;
  }

  /// The combined name and namespace of a schema.
  string getFullname() const {
    return fullname;
  }

  bool opEquals(const Name n) const {
    return n !is null && fullname == n.fullname;
  }

  override
  bool opEquals(Object o) const {
    return this.opEquals(cast(Name) o);
  }

  ///
  @trusted unittest {
    assert(new Name("com.example.bob", null) == new Name("bob", "com.example"));
  }

  override
  size_t toHash() const nothrow @trusted {
    if (fullname is null) return 0;
    size_t hash = 0;
    foreach (c; fullname)
      hash = hash * 11 + c;
    return hash;
  }

  ///
  unittest {
    assert((new Name("com.example.bob", null)).toHash == (new Name("bob", "com.example")).toHash);
    assert((new Name("com.example.bob", null)).toHash != (new Name("bob2", "com.example")).toHash);
  }

  override
  string toString() const {
    return fullname;
  }
}
