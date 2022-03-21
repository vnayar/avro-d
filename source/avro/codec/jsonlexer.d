/// Logic for parsing JSON
module avro.codec.jsonlexer;

import std.conv;
import std.traits : isSomeChar;
import std.uni : isNumber, isWhite, isAlpha, isAlphaNum;
import std.range;

import avro.codec.jsonutil : decodeJsonString;

@safe:

/// Exceptions related to incremental parsing in [JsonLexer].
class JsonLexException : Exception {
  this(string msg, Throwable nextInChain = null, string file = __FILE__, size_t line = __LINE__) {
    super(msg, file, line, nextInChain);
  }
}

enum Token {
  NULL,
  BOOL,
  LONG,
  DOUBLE,
  STRING,
  ARRAY_START,
  ARRAY_END,
  OBJECT_START,
  OBJECT_END
}

/// A JSON lexer that allows tokens to be processed as they are read.
class JsonLexer(IRangeT)
if (isInputRange!(IRangeT) && isSomeChar!(ElementType!IRangeT))
{
  size_t line() const {
    return lineNo;
  }

 public:
  this(IRangeT iRange) {
    this.iRange = iRange;
  }

  /// Read the next token from the input data and return it.
  Token advance() {
    if (!peeked) {
      curToken = doAdvance();
    } else {
      peeked = false;
    }
    return curToken;
  }

  /// View the next Token without consuming it.
  Token peek() {
    if (!peeked) {
      curToken = doAdvance();
      peeked = true;
    }
    return curToken;
  }

  /// Test that a token type is present, and throw an error if not.
  void expectToken(Token tk) {
    if (advance() != tk) {
      if (tk == Token.DOUBLE) {
        if (cur() == Token.STRING
            && (sv == "Infinity" || sv == "-Infinity" || sv == "NaN")) {
          curToken = Token.DOUBLE;
          dv = sv == "Infinity" ? double.infinity
              : sv == "-Infinity" ? -double.infinity : double.nan;
          return;
        } else if (cur() == Token.LONG) {
          dv = lv.to!double;
          return;
        }
      }
      throw new JsonLexException("Incorrect token in the stream. Expected: " ~ tk.to!string
          ~ ", found " ~ cur().to!string);
    }
  }

  /// Returns the boolean value of the most recently read token.
  bool boolValue() const {
    return bv;
  }

  /// Returns the most recently read token.
  Token cur() const {
    return curToken;
  }

  /// Returns the double value of the most recently read token.
  double doubleValue() const {
    return dv;
  }

  /// Returns the long value of the most recently read token.
  long longValue() const {
    return lv;
  }

  string rawString() const {
    return sv;
  }

  string stringValue() const {
    return decodeJsonString(sv);
  }

 private:
  enum State {
    VALUE,   // Expect a data type
    ARRAY_0,  // Expect a data type or ']'
    ARRAY_N,  // Expect a ',' or ']'
    OBJECT_0, // Expect a string or a '}'
    OBJECT_N, // Expect a ',' or '}'
    KEY      // Expect a ':'
  }
  State[] stateStack;
  State curState;
  bool hasNext = false;
  dchar nextChar;
  bool peeked = false;

  IRangeT iRange;
  Token curToken;
  bool bv;
  long lv;
  double dv;
  string sv;
  size_t lineNo = 1;

  /// Reads the input range for the next character (skipping whitespace).
  dchar next() {
    dchar ch = hasNext ? nextChar : ' ';
    while (isWhite(ch)) {
      if (ch == '\n') {
        lineNo++;
      }
      ch = iRange.front();
      iRange.popFront();
    }
    hasNext = false;
    return ch;
  }

  /// Reads input characters, updates the lexer state, and returns the next [Token].
  Token doAdvance() {
    dchar ch = next();
    if (ch == ']') {
      if (curState == State.ARRAY_0 || curState == State.ARRAY_N) {
        curState = stateStack.back();
        stateStack.popBack();
        return Token.ARRAY_END;
      } else {
        throw unexpected(ch);
      }
    } else if (ch == '}') {
      if (curState == State.OBJECT_0 || curState == State.OBJECT_N) {
        curState = stateStack.back();
        stateStack.popBack();
        return Token.OBJECT_END;
      } else {
        throw unexpected(ch);
      }
    } else if (ch == ',') {
      if (curState != State.OBJECT_N && curState != State.ARRAY_N) {
        throw unexpected(ch);
      }
      if (curState == State.OBJECT_N) {
        curState = State.OBJECT_0;
      }
      ch = next();
    } else if (ch == ':') {
      if (curState != State.KEY) {
        throw unexpected(ch);
      }
      curState = State.OBJECT_N;
      ch = next();
    }

    if (curState == State.OBJECT_0) {
      if (ch != '"') {
        throw unexpected(ch);
      }
      curState = State.KEY;
    } else if (curState == State.ARRAY_0) {
      curState = State.ARRAY_N;
    }

    switch (ch) {
      case '[':
        stateStack ~= curState;
        curState = State.ARRAY_0;
        return Token.ARRAY_START;
      case '{':
        stateStack ~= curState;
        curState = State.OBJECT_0;
        return Token.OBJECT_START;
      case '"':
        return tryString();
      case 't':
        bv = true;
        return tryLiteral("rue", 3, Token.BOOL);
      case 'f':
        bv = false;
        return tryLiteral("alse", 4, Token.BOOL);
      case 'n':
        return tryLiteral("ull", 3, Token.NULL);
      default:
        if (isNumber(ch) || ch == '-') {
          return tryNumber(ch);
        } else {
          throw unexpected(ch);
        }
    }
  }

  /**
     Attempts to process a literal string of expected output, such as "null" or "false".
  */
  Token tryLiteral(string exp, size_t n, Token tk) {
    dchar[] c = iRange.takeExactly(n).array;
    iRange.popFrontN(n);
    for (size_t i = 0; i < n; ++i) {
      if (c[i] != exp[i]) {
        throw unexpected(c[i]);
      }
    }
    if (!iRange.empty()) {
      nextChar = iRange.front();
      iRange.popFront();
      if (isAlphaNum(nextChar)) {
        throw unexpected(nextChar);
      }
      hasNext = true;
    }
    return tk;
  }

  /// Consumes characters in order to identify a number.
  Token tryNumber(dchar ch) {
    sv = "";
    sv ~= ch;

    hasNext = false;
    int state = (ch == '-') ? 0 : (ch == '0') ? 1 : 2;
    for (;;) {
      switch (state) {
        case 0:  // A negative number.
          if (!iRange.empty()) {
            ch = iRange.front();
            iRange.popFront();
            if (isNumber(ch)) {
              state = (ch == '0') ? 1 : 2;
              sv ~= ch;
              continue;
            }
            hasNext = true;
          }
          break;
        case 1: // Leading 0, possible decimal or scientific notation.
          if (!iRange.empty()) {
            ch = iRange.front();
            iRange.popFront();
            if (ch == '.') {
              state = 3;
              sv ~= ch;
              continue;
            } else if (ch == 'e' || ch == 'E') {
              sv ~= ch;
              state = 5;
              continue;
            }
            hasNext = true;
          }
          break;
        case 2:
          if (!iRange.empty()) {
            ch = iRange.front();
            iRange.popFront();
            if (isNumber(ch)) {
              sv ~= ch;
              continue;
            } else if (ch == '.') {
              state = 3;
              sv ~= ch;
              continue;
            } else if (ch == 'e' || ch == 'E') {
              sv ~= ch;
              state = 5;
              continue;
            }
            hasNext = true;
          }
          break;
        case 3: // Checking for decimal fraction.
        case 6: // Checking for exponent value.
          if (!iRange.empty()) {
            ch = iRange.front();
            iRange.popFront();
            if (isNumber(ch)) {
              sv ~= ch;
              state++;
              continue;
            }
            hasNext = true;
          }
          break;
        case 4:  // Reading decimal fraction / exponent.
          if (!iRange.empty()) {
            ch = iRange.front();
            iRange.popFront();
            if (isNumber(ch)) {
              sv ~= ch;
              continue;
            } else if (ch == 'e' || ch == 'E') {
              sv ~= ch;
              state = 5;
              continue;
            }
            hasNext = true;
          }
          break;
        case 5:  // Start of exponent value.
          if (!iRange.empty()) {
            ch = iRange.front();
            iRange.popFront();
            if (ch == '+' || ch == '-') {
              sv ~= ch;
              state = 6;
              continue;
            } else if (isNumber(ch)) {
              sv ~= ch;
              state = 7;
              continue;
            }
            hasNext = true;
          }
          break;
        case 7:  // Reading numerical value of exponent.
          if (!iRange.empty()) {
            ch = iRange.front();
            iRange.popFront();
            if (isNumber(ch)) {
              sv ~= ch;
              continue;
            }
            hasNext = true;
          }
          break;
        default:
          throw new JsonLexException("Unexpected JSON lex state");
      }
      if (state == 1 || state == 2 || state == 4 || state == 7) {
        if (hasNext) {
          nextChar = ch;
        }
        if (state == 1 || state == 2) {
          lv = parse!long(sv);
          return Token.LONG;
        } else {
          dv = parse!double(sv);
          return Token.DOUBLE;
        }
      } else {
        if (hasNext) {
          throw unexpected(ch);
        } else {
          throw new JsonLexException("Unexpected EOF");
        }
      }
    }
  }

  ///
  Token tryString() {
    sv = "";
    for (;;) {
      dchar ch = iRange.front();
      iRange.popFront();
      if (ch == '"') {
        return Token.STRING;
      } else if (ch == '\\') {
        ch = iRange.front();
        iRange.popFront();
        switch (ch) {
          case '"':
          case '\\':
          case '/':
          case 'b':
          case 'f':
          case 'n':
          case 'r':
          case 't':
            sv ~= '\\';
            sv ~= ch;
            break;
          case 'u':
          case 'U': {
            uint n = 0;
            dchar[] e = iRange.takeExactly(4).array;
            iRange.popFrontN(4);
            sv ~= '\\';
            sv ~= ch;
            foreach (char c; e) {
              n *= 16;
              if (isNumber(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
                sv ~= c;
              } else {
                throw unexpected(ch);
              }
            }
          } break;
          default:
            throw unexpected(ch);
        }
      } else {
        sv ~= ch;
      }
    }
  }

  JsonLexException unexpected(dchar ch) {
    return new JsonLexException("Unexpected character in json '" ~ ch.to!string ~ "'.");
  }
}

/// Convenience function to create a [JsonLexer] and infer the range type.
auto jsonLexer(IRangeT)(IRangeT iRange) {
  return new JsonLexer!IRangeT(iRange);
}

///
@trusted unittest {
  import std.exception : assertNotThrown;

  struct Test {
    string text;
    Token expected;
    bool function(JsonLexer!string) lexerValidator;
  }

  Test[] tests = [
      Test("null", Token.NULL, (p) => true),
      Test("  true", Token.BOOL, (p) => p.boolValue() == true),
      Test("false ", Token.BOOL, (p) => p.boolValue() == false),
      Test("7", Token.LONG, (p) => p.longValue() == 7),
      Test("-6", Token.LONG, (p) => p.longValue() == -6),
      Test("34.56", Token.DOUBLE, (p) => p.doubleValue() == 34.56),
      Test("-4.567", Token.DOUBLE, (p) => p.doubleValue() == -4.567),
      Test("1.23e20", Token.DOUBLE, (p) => p.doubleValue() == 1.23e20),
      Test("-1.23e+20", Token.DOUBLE, (p) => p.doubleValue() == -1.23e20),
      Test("-1.23e-20", Token.DOUBLE, (p) => p.doubleValue() == -1.23e-20),
      Test("\"b\\t\\u25BDb\"", Token.STRING, (p) => p.rawString() == "b\\t\\u25BDb"),
      Test("\"b\\t\\u25BDb\"", Token.STRING, (p) => p.stringValue() == "b\t▽b"),
    ];
  foreach (size_t i, Test test; tests) {
    auto lexer = jsonLexer(test.text);
    assertNotThrown(lexer.expectToken(test.expected), "Invalid token in test " ~ i.to!string);
    assert(test.lexerValidator(lexer), "Lexer invalid in test " ~ i.to!string);
  }
}

///
unittest {
  auto lexer = jsonLexer(" [\"b[ob\" ,\"h]am\"] ");
  lexer.expectToken(Token.ARRAY_START);
  lexer.expectToken(Token.STRING);
  lexer.expectToken(Token.STRING);
  lexer.expectToken(Token.ARRAY_END);
}

///
unittest {
  auto lexer = jsonLexer("{\n  \"a\" : \"b[]{}ob\"\n ,\"b\" : \"h{[]}m\"} ");
  lexer.expectToken(Token.OBJECT_START);
  lexer.expectToken(Token.STRING);
  lexer.expectToken(Token.STRING);
  lexer.expectToken(Token.STRING);
  lexer.expectToken(Token.STRING);
  lexer.expectToken(Token.OBJECT_END);
}

///
unittest {
  import std.exception : assertThrown;
  auto lexer = jsonLexer(" [\"b[ob\" ,\"h]am\"] ");
  assertThrown!JsonLexException(lexer.expectToken(Token.OBJECT_START));
}

///
unittest {
  auto lexer = jsonLexer("{\n  \"a\" : \"b[]{}ob\"\n ,\"b\" : \"h{[]}m\"} ");
  assert(lexer.peek() == Token.OBJECT_START);
  assert(lexer.peek() == Token.OBJECT_START);
  assert(lexer.advance() == Token.OBJECT_START);
  assert(lexer.advance() == Token.STRING);
  assert(lexer.advance() == Token.STRING);
  assert(lexer.peek() == Token.STRING);
  assert(lexer.peek() == Token.STRING);
  assert(lexer.advance() == Token.STRING);
  assert(lexer.advance() == Token.STRING);
  assert(lexer.advance() == Token.OBJECT_END);
}
