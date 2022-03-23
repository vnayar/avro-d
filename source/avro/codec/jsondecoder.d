/// A decoder for processing JSON Avro input.
module avro.codec.jsondecoder;

import std.conv;
import std.algorithm : map;
import std.traits : isSomeChar;
import std.uni : isNumber, isWhite, isAlpha, isAlphaNum;
import std.range;

import avro.schema : Schema;
import avro.type : Type;
import avro.codec.decoder : Decoder;
import avro.codec.jsonlexer : jsonLexer, JsonLexer, Token;

version (unittest) {
  import std.exception : assertThrown, assertNotThrown;
  import avro.codec.jsonlexer : JsonLexException;
}

@safe:

/// A JSON pull-parser that allows tokens to be processed as they are read.
class JsonDecoder(IRangeT) : Decoder
if (isInputRange!(IRangeT) && isSomeChar!(ElementType!IRangeT)) {
  JsonLexer!IRangeT lexer;

  this(IRangeT iRange) {
    this.lexer = jsonLexer(iRange);
  }

  override
  void readNull() {
    lexer.expectToken(Token.NULL);
  }

  ///
  unittest {
    string data = "null";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assertNotThrown(readNull());
    }
  }

  override
  bool readBoolean() {
    lexer.expectToken(Token.BOOL);
    return lexer.boolValue();
  }

  ///
  unittest {
    string data = "true false truee";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readBoolean() == true);
      assert(readBoolean() == false);
      assertThrown!JsonLexException(readBoolean());
    }
  }

  override
  long readLong() {
    lexer.expectToken(Token.LONG);
    return lexer.longValue();
  }

  ///
  unittest {
    string data = "12341 123.21";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readLong() == 12341L);
      assertThrown!JsonLexException(readLong());
    }
  }

  override
  int readInt() {
    return readLong().to!int;
  }

  override
  double readDouble() {
    lexer.expectToken(Token.DOUBLE);
    return lexer.doubleValue();
  }

  ///
  unittest {
    string data = "12341 123.21";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readDouble() == 12341.0);
      assert(readDouble() == 123.21);
    }
  }

  override
  float readFloat() {
    return readDouble().to!float;
  }

  override
  string readString() {
    lexer.expectToken(Token.STRING);
    return lexer.stringValue();
  }

  ///
  unittest {
    string data = "\"ham\" \"\\u26A1\" ham";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readString() == "ham");
      assert(readString() == "⚡");
      assertThrown!JsonLexException(readString());
    }
  }

  override
  void skipString() {
    readString();
  }

  ubyte[] toBytes(string s) {
    return s.map!(to!ubyte).array;
  }

  override
  ubyte[] readBytes() {
    lexer.expectToken(Token.STRING);
    return toBytes(lexer.stringValue());
  }

  ///
  unittest {
    string data = "\"abc\\u0000\\t\\u0001\"";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readBytes() == [0x61, 0x62, 0x63, 0x00, 0x09, 0x01]);
    }
  }

  override
  void skipBytes() {
    readBytes();
  }

  override
  ubyte[] readFixed(size_t length) {
    ubyte[] bytes = readBytes();
    if (bytes.length != length) {
      throw new Exception("Expected fixed with " ~ length.to!string ~ " bytes, "
          ~ "but got " ~ bytes.length.to!string ~ " byyes.");
    }
    return bytes;
  }

  ///
  unittest {
    string data = "\"abc\\u0000\\t\\u0001\" \"abc\\u0000\\t\\u0001\"";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readFixed(6) == [0x61, 0x62, 0x63, 0x00, 0x09, 0x01]);
      assertThrown(readFixed(5));
    }
  }

  override
  void skipFixed(size_t length) {
    readFixed(length);
  }

  override
  size_t readEnum(const Schema enumSchema)
  in (enumSchema.getType() == Type.ENUM)
  {
    lexer.expectToken(Token.STRING);
    return enumSchema.getEnumOrdinal(lexer.stringValue());
  }

  ///
  unittest {
    import avro.name : Name;
    import avro.schema : EnumSchema;
    Schema schema = new EnumSchema(new Name("mytype", null), "", ["A", "B", "C"], "A");
    string data = "\"A\" \"B\" \"D\"";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readEnum(schema) == 0);
      assert(readEnum(schema) == 1);
      assertThrown(readEnum(schema));
    }
  }

  override
  void readRecordStart() {
    lexer.expectToken(Token.OBJECT_START);
  }

  ///
  unittest {
    string data = `{ "A": 3, "B": { "C": 4 } }`;
    auto decoder = jsonDecoder(data);
    with (decoder) {
      readRecordStart();
      readRecordKey();
      assert(readInt() == 3);
      readRecordKey();
      readRecordStart();
      readRecordKey();
      assert(readInt() == 4);
      readRecordEnd();
      readRecordEnd();
    }
  }

  override
  string readRecordKey() {
    lexer.expectToken(Token.STRING);
    return lexer.stringValue();
  }

  override
  void readRecordEnd() {
    lexer.expectToken(Token.OBJECT_END);
  }

  override
  size_t readArrayStart() {
    lexer.expectToken(Token.ARRAY_START);
    return readArrayNext();
  }

  ///
  unittest {
    string data = "[ [1, 2], [3, 4] ]";
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readArrayStart() == 1);
      assert(readArrayStart() == 1);
      assert(readInt() == 1);
      assert(readArrayNext() == 1);
      assert(readInt() == 2);
      assert(readArrayNext() == 0);
      assert(readArrayStart() == 1);
      assert(readInt() == 3);
      assert(readArrayNext() == 1);
      assert(readInt() == 4);
      assert(readArrayNext() == 0);
      assert(readArrayNext() == 0);
    }
  }

  override
  size_t readArrayNext() {
    if (lexer.peek() == Token.ARRAY_END) {
        lexer.advance();
        return 0;
    }
    return 1;
  }

  /// Assuming the start of an array/object has been read, keep reading until its end is found.
  void skipComposite() {
    size_t level = 0;
    for (;;) {
      switch (lexer.advance()) {
        case Token.ARRAY_START:
        case Token.OBJECT_START:
          ++level;
          continue;
        case Token.ARRAY_END:
        case Token.OBJECT_END:
          if (level == 0) {
            return;
          }
          --level;
          continue;
        default:
          continue;
      }
    }
  }

  override
  long skipArray() {
    lexer.expectToken(Token.ARRAY_START);
    skipComposite();
    return 0;
  }

  override
  size_t readMapStart() {
    lexer.expectToken(Token.OBJECT_START);
    return readMapNext();
  }

  ///
  unittest {
    string data = `{ "A": 3, "B": { "C": 4 } }`;
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readMapStart() == 1);
      assert(readString() == "A");
      assert(readInt() == 3);
      assert(readMapNext() == 1);
      assert(readString() == "B");
      assert(readMapStart() == 1);
      assert(readString() == "C");
      assert(readInt() == 4);
      assert(readMapNext() == 0);
      assert(readMapNext() == 0);
    }
  }

  override
  size_t readMapNext() {
    if (lexer.peek() == Token.OBJECT_END) {
      lexer.advance();
      return 0;
    }
    return 1;
  }

  override
  size_t skipMap() {
    lexer.expectToken(Token.OBJECT_START);
    skipComposite();
    return 0;
  }

  override
  size_t readUnionIndex(const Schema unionSchema)
  in (unionSchema.getType() == Type.UNION)
  {
    size_t result;
    if (lexer.peek() == Token.NULL) {
      result = unionSchema.getIndexNamed("null");
    } else {
      lexer.expectToken(Token.OBJECT_START);
      lexer.expectToken(Token.STRING);
      result = unionSchema.getIndexNamed(lexer.stringValue());
    }
    return result;
  }

  ///
  unittest {
    import avro.name : Name;
    import avro.schema : UnionSchema, NullSchema, BooleanSchema, IntSchema;

    Schema schema = new UnionSchema([new NullSchema(), new BooleanSchema(), new IntSchema()]);
    string data = `null {"boolean": true} {"int": 3}`;
    auto decoder = jsonDecoder(data);
    with (decoder) {
      assert(readUnionIndex(schema) == 0);
      readNull();
      assert(readUnionIndex(schema) == 1);
      assert(readBoolean() == true);
      readUnionEnd();
      assert(readUnionIndex(schema) == 2);
      assert(readInt() == 3);
      readUnionEnd();
    }
  }

  override
  void readUnionEnd() {
    lexer.expectToken(Token.OBJECT_END);
  }
}

/// A helper function for constructing a [JsonDecoder] with inferred template arguments.
auto jsonDecoder(IRangeT)(IRangeT iRange) {
  return new JsonDecoder!(IRangeT)(iRange);
}

///
unittest {
  string data = "null";
  auto decoder = jsonDecoder(data);
  with (decoder) {
    assertNotThrown(readNull());
  }
}
