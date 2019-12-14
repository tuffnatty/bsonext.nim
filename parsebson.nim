import streams
import sugar

import bson

export bson


type
  BsonParser* = object
    s: Stream
    name*: string
    case kind*: BsonKind
      of BsonKindDouble:
        valueFloat64*: float64
      of BsonKindBinary:
        subType*: BsonSubtype
        binBuffer*: string
      of BsonKindBool:
        valueBool*: bool
      of BsonKindRegexp:
        regex*: string
        options*: string
      of BsonKindDBPointer, BsonKindOid:
        valueLo*: int64
        valueHi*: int32
        refcol*: string
      of BsonKindInt32:
        valueInt32*: int32
      of BsonKindInt64, BsonKindTimeUTC:
        valueInt64*: int64
      of BsonKindTimestamp:
        valueUint64*: uint64
      else:
        buffer*: string


proc readStr*(s: Stream, length: int, result: var string) =
  result.setLen(length)
  if length != 0:
    var L = readData(s, addr(result[0]), length)
    if L != length: setLen(result, L)

proc open*(parser: var BsonParser, s: Stream) =
  parser.s = s
  discard s.readInt32

proc next*(parser: var BsonParser) =
  let s = parser.s
  parser = BsonParser(s: s, kind: s.readChar)
  if parser.kind != BsonKindUnknown:
    discard s.readLine(parser.name)
  case parser.kind:
  of BsonKindDouble:
    parser.valueFloat64 = s.readFloat64
  of BsonKindStringUTF8, BsonKindJSCode, BsonKindDeprecated:
    s.readStr(s.readInt32() - 1, parser.buffer)
    discard s.readChar
  of BsonKindDocument, BsonKindArray:
    discard s.readInt32
  of BsonKindJSCodeWithScope:
    s.readStr(s.readInt32() - 1, parser.buffer)
    discard s.readChar
    discard s.readInt32
  of BsonKindBinary:
    let binSize = s.readInt32
    parser.subType = s.readChar.BsonSubtype
    if binSize > 0:
      s.readStr(binSize, parser.buffer)
    else:
      parser.buffer.setLen(0)
  of BsonKindOid:
    parser.valueLo = s.readInt64
    parser.valueHi = s.readInt32
  of BsonKindBool:
    parser.valueBool = s.readChar() == 0.char
  of BsonKindTimeUTC, BsonKindInt64:
    parser.valueInt64 = s.readInt64
  of BsonKindTimestamp:
    parser.valueUint64 = s.readUInt64
  of BsonKindRegexp:
    parser.regex = s.readLine
    parser.options = s.readLine
  of BsonKindDBPointer:
    parser.refcol = s.readStr(s.readInt32 - 1)
    discard s.readChar
    parser.valueLo = s.readInt64
    parser.valueHi = s.readInt32
  of BsonKindInt32:
    parser.valueInt32 = s.readInt32
  of BsonKindUnknown, BsonKindUndefined, BsonKindNull, BsonKindMinimumKey, BsonKindMaximumKey:
    discard
  else:
    dump parser.kind
    assert false, "unknown Bson kind tag"
