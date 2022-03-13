module avro.codec.jsonutil;

import std.range : empty, popFront, popFrontN, front, takeExactly;
import std.format : format, formattedRead, FormatException;
import std.array : appender;

/**
   Encodes a string so that it may be used as a JSON-string value.

   For example, in the following JSON object, consider values of `STR`.
   ---
   { "a" : "STR"}
   ---

   If the value of STR was `ham", "b": "extra`, then once substitued into JSON data, the
   new value would be:
   ---
   { "a" : "ham", "b": "extra"}
   ---

   To prevent the injection of data like this, strings must be JSON encoded before being applied.
   https://www.json.org/json-en.html

   Special characters like `"` and `\n` are preceded with an escape character `\`, which must be
   reversed when the value is being decoded.

   Thus, the value `ham", "b": "extra` would become `ham\", \"b\": \"extra`, resulting in a JSON
   value that is safe for decoders.
   ---
   { "a": "ham\", \"b\": \"extra" }
   ---

   See_Also: https://www.json.org/json-en.html
*/
string encodeJsonString(string str) {
  auto buf = appender!(string)();
  foreach (dchar ch; str) {
    switch (ch) {
      case '"':
        buf ~= "\\\"";
        break;
      case '\\':
        buf ~= "\\\\";
        break;
      case '/':
        buf ~= "\\/";
        break;
      case '\b':
        buf ~= "\\b";
        break;
      case '\f':
        buf ~= "\\f";
        break;
      case '\n':
        buf ~= "\\n";
        break;
      case '\r':
        buf ~= "\\r";
        break;
      case '\t':
        buf ~= "\\t";
        break;
      default:
        // Reference: https://www.unicode.org/versions/Unicode5.1.0/
        if ((ch >= '\u0000' && ch <= '\u001F') || (ch >= '\u007F' && ch <= '\u009F')
            || (ch >= '\u2000' && ch <= '\u20FF')) {
          string hex = format!("%04X")(ch);
          buf ~= "\\u";
          buf ~= hex;
        } else {
          buf ~= ch;
        }
    }
  }
  return buf[];
}

///
unittest {
  string val;
  assert((val = encodeJsonString("\b\t\"/j'\f\n\r\0")) == "\\b\\t\\\"\\/j'\\f\\n\\r\\u0000", val);
  assert((val = encodeJsonString("Grüßen")) == "Grüßen", val);
  assert((val = encodeJsonString("あいさつ")) == "あいさつ", val);
}

/**
   Removes the encoding done for JSON strings so that they are represented as a D string.
*/
string decodeJsonString(string str) {
  auto buf = appender!(string)();
  bool escaped = false;
  dchar ch;
  while (!str.empty()) {
    ch = str.front();
    popFront(str);
    if (ch == '\\') {
      escaped = true;
      continue;
    }
    if (escaped) {
      escaped = false;
      switch (ch) {
        case 'b':
          buf ~= '\b';
          break;
        case 'f':
          buf ~= '\f';
          break;
        case 'n':
          buf ~= '\n';
          break;
        case 'r':
          buf ~= '\r';
          break;
        case 't':
          buf ~= '\t';
          break;
        case 'u':
          if (str.length < 4)
            throw new FormatException("JSON Parse exception, \\u should have 4 digits.");
          formattedRead!("%x")(str.takeExactly(4), &ch);
          str.popFrontN(4);
          buf ~= ch;
          break;
        default:
          buf ~= ch;
      }
    } else {
      buf ~= ch;
    }
  }
  return buf[];
}

///
unittest {
  string val;
  assert((val = decodeJsonString("\\b\\t\\\"\\/j'\\f\\n\\r\\u0000")) == "\b\t\"/j'\f\n\r\0", val);
  assert((val = decodeJsonString("Grüßen")) == "Grüßen", val);
  assert((val = decodeJsonString("あいさつ")) == "あいさつ", val);
}

