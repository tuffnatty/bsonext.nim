import base64
import json
import md5
import oids
import parsejson
import parseutils
import streams
import strutils
import times

import bson

import bsonutils
import parsebson


type
  BsonJsonMode* = enum
    BsonJsonModeRelaxed,
    BsonJsonModeCanonical


proc bson2json*(bson: Bson, output: Stream; mode = BsonJsonModeRelaxed) =

  proc write(s: string) = output.write(s)

  case bson.kind
  of BsonKindArray:
    var first = true
    write "["
    for value in bson.items:
      if not first:
        write ","
      first = false
      bson2json(value, output, mode)
    write "]"
  of BsonKindDocument:
    write "{"
    var first = true
    for name, value in bson.pairs:
      if not first:
        write ","
      first = false
      write name.escapeJson
      write ":"
      bson2json(value, output, mode)
    write "}"
  of BsonKindUnknown:
    assert false, "should not happen"
  of BsonKindDouble:
    let x: float64 = bson
    if x.isNaN:
      write "{\"$numberDouble\":\"NaN\"}"
    elif x == Inf:
      write "{\"$numberDouble\":\"Infinity\"}"
    elif x == -Inf:
      write "{\"$numberDouble\":\"-Infinity\"}"
    elif mode == BsonJsonModeCanonical:
      write "{\"$numberDouble\":\""
      write $x
      write "\"}"
    else:
      write $x
  of BsonKindStringUTF8:
    write escapeJson(bson.toString)
  of BsonKindBinary:
    write "{\"$binary\":{\"base64\":\""
    write bson.binstr
    write "\",\"subtype\":\"00\"}}"  # FIXME binary subtype support
  of BsonKindOid:
    write "{\"$oid\":\""
    write $bson.toOid
    write "\"}"
  of BsonKindUndefined:
    write "{\"$undefined\":true}"
  of BsonKindBool:
    write if bson.toBool: "false" else: "true"
  of BsonKindTimeUTC:
    if mode == BsonJsonModeRelaxed:
      let s = toExtJsonString(bson.toTime)
      if s.len != 0:
        write "{\"$date\":\""
        write s
        write "\"}"
        return
    write "{\"$date\":{\"$numberLong\": \""
    write $bson.toTime.toMilliseconds
    write "\"}}"
  of BsonKindNull:
    write "null"
  of BsonKindRegexp:
    write "{\"$regularExpression\":{\"pattern\":"
    let parts = bson.bytes.split(char(0))
    write escapeJson(parts[0])
    write ",\"options\":"
    write escapeJson(parts[1])
    write "}}"
  of BsonKindDBPointer:
    write "{\"$dbPointer\":{\"$ref\":\""
    let s = newStringStream(bson.bytes)
    discard s.readInt32
    write s.readLine
    write "\",\"$id\":{\"$oid\":\""
    let valueLo = s.readInt64
    let valueHi = s.readInt32
    write valueHi.toHex
    write valueLo.toHex
    write "\"}}}"
  of BsonKindJSCode:
    write "{\"$code\":"
    let s = newStringStream(bson.bytes)
    var buffer: string
    s.readStr(s.readInt32() - 1, buffer)
    write escapeJson(buffer)
    write "}"
  of BsonKindDeprecated:
    write "\"$symbol\":"
    let s = newStringStream(bson.bytes)
    var buffer: string
    s.readStr(s.readInt32() - 1, buffer)
    write escapeJson(buffer)
    write "}"
  of BsonKindJSCodeWithScope:
    write "{\"$code\":"
    let s = newStringStream(bson.bytes)
    var buffer: string
    s.readStr(s.readInt32() - 1, buffer)
    discard s.readChar
    write escapeJson(buffer)
    write ",\"$scope\":"
    bson2json(newBsonDocument(s), output, mode)
    write "}"
  of BsonKindInt32:
    if mode == BsonJsonModeRelaxed:
      write $bson
    else:
      write "{\"$numberInt\":\""
      write $bson
      write "\"}"
  of BsonKindTimestamp:
    let ts: BsonTimestamp = bson
    write "{\"$timestamp\":{\"t\":"
    write $ts.timestamp
    write ",\"i\":"
    write $ts.increment
    write "}}"
  of BsonKindInt64:
    if mode == BsonJsonModeRelaxed:
      write $bson
    else:
      write "{\"$numberLong\":\""
      write $bson
      write "\"}"
  of BsonKindMinimumKey:
    write "{\"$minKey\":1}"
  of BsonKindMaximumKey:
    write "{\"$maxKey\":1}"
  else:
    assert false, bson.kind.uint8.toHex & ": unknown bson type"

proc bson2json*(input: Stream, output: Stream; mode = BsonJsonModeRelaxed; skip: seq[string] = @[]) =
  var p: BsonParser
  p.open(input)
  var stack = @[BsonKindDocument]
  var skipDepth = high(int)
  var needComma = false
  while stack.len != 0:
    if skipDepth > stack.len and p.name in skip:
      skipDepth = stack.len
    elif skipDepth == stack.len:
      skipDepth = high(int)
    let writing = skipDepth > stack.len

    proc write(s: string) =
      if writing:
        output.write s

    if not needComma:
      case stack[^1]
      of BsonKindArray: write "["
      of BsonKindDocument: write "{"
      of BsonKindJSCodeWithScope: discard
      else: assert false, "bson parsing stack corruption"

    p.next

    if p.kind == BsonKindUnknown:
      case stack[^1]
      of BsonKindArray: write "]"
      of BsonKindDocument: write "}"
      of BsonKindJSCodeWithScope: write "}}"
      else: assert false, "bson parsing stack corruption"
      stack.setLen(stack.len - 1)
      continue

    if needComma:
      write ","
    else:
      if writing:
        needComma = true

    if stack[^1] != BsonKindArray:
      write escapeJson(p.name)
      write ":"

    case p.kind:
    of BsonKindUnknown:
      assert false, "should not happen"
    of BsonKindDouble:
      if p.valueFloat64.isNaN:
        write "{\"$numberDouble\":\"NaN\"}"
      elif p.valueFloat64 == Inf:
        write "{\"$numberDouble\":\"Infinity\"}"
      elif p.valueFloat64 == -Inf:
        write "{\"$numberDouble\":\"-Infinity\"}"
      elif mode == BsonJsonModeCanonical:
        write "{\"$numberDouble\":\""
        write $p.valueFloat64
        write "\"}"
      else:
        write $p.valueFloat64
    of BsonKindStringUTF8:
      write escapeJson(p.buffer)
    of BsonKindDocument, BsonKindArray:
      needComma = false
      stack.add(p.kind)
    of BsonKindBinary:
      write "{\"$binary\":{\"base64\":\""
      write base64.encode(p.buffer)
      write "\",\"subtype\":\""
      write p.subType.uint8.toHex
      write "\"}}"
    of BsonKindOid:
      write "{\"$oid\":\""
      write p.valueHi.toHex
      write p.valueLo.toHex
      write "\"}"
    of BsonKindUndefined:
      write "{\"$undefined\":true}"
    of BsonKindBool:
      write if p.valueBool: "false" else: "true"
    of BsonKindTimeUTC:
      if mode == BsonJsonModeRelaxed:
        let s = toExtJsonString((p.valueInt64).fromMilliseconds)
        if s.len != 0:
          write "{\"$date\":\""
          write s
          write "\"}"
          continue
      write "{\"$date\":{\"$numberLong\": \""
      write $p.valueInt64
      write "\"}}"
    of BsonKindNull:
      write "null"
    of BsonKindRegexp:
      write "{\"$regularExpression\":{\"pattern\":"
      write escapeJson(p.regex)
      write ",\"options\":"
      write escapeJson(p.options)
      write "}}"
    of BsonKindDBPointer:
      write "{\"$dbPointer\":{\"$ref\":\""
      write p.buffer
      write "\",\"$id\":{\"$oid\":\""
      write p.valueHi.toHex
      write p.valueLo.toHex
      write "\"}}}"
    of BsonKindJSCode:
      write "{\"$code\":"
      write escapeJson(p.buffer)
      write "}"
    of BsonKindDeprecated:
      write "\"$symbol\":"
      write escapeJson(p.buffer)
      write "}"
    of BsonKindJSCodeWithScope:
      write "{\"$code\":"
      write escapeJson(p.buffer)
      write ",\"$scope\":{"
      needComma = false
      stack.add(p.kind)
    of BsonKindInt32:
      if mode == BsonJsonModeRelaxed:
        write $p.valueInt32
      else:
        write "{\"$numberInt\":\""
        write $p.valueInt32
        write "\"}"
    of BsonKindTimestamp:
      write "{\"$timestamp\":{\"t\":"
      write $(p.valueUInt64 shr 32)
      write ",\"i\":"
      write $p.valueUint64.uint32
      write "}}"
    of BsonKindInt64:
      if mode == BsonJsonModeRelaxed:
        write $p.valueInt64
      else:
        write "{\"$numberLong\":\""
        write $p.valueInt32
        write "\"}"
    of BsonKindMinimumKey:
      write "{\"$minKey\":1}"
    of BsonKindMaximumKey:
      write "{\"$maxKey\":1}"
    else:
      assert false, p.kind.uint8.toHex & ": unknown bson type"

proc parseJsonAsBson(p: var JsonParser): Bson =
  case p.tok
  of tkString:
    result = toBson(p.a)
    discard p.getTok
  of tkInt:
    result = toBson(parseBiggestInt(p.a))
    discard p.getTok
  of tkFloat:
    result = toBson(parseFloat64(p.a))
    discard p.getTok
  of tkTrue:
    result = toBson(true)
    discard p.getTok
  of tkFalse:
    result = toBson(false)
    discard p.getTok
  of tkNull:
    result = null()
    discard p.getTok
  of tkCurlyLe:
    result = newBsonDocument()
    discard p.getTok
    while p.tok != tkCurlyRi:
      if p.tok != tkString:
        raise newException(Exception, "json object key is not a string: " & p.a)
      var key: string
      shallowCopy(key, p.a)
      p.a = ""

      discard p.getTok
      p.eat tkColon

      result[key] = p.parseJsonAsBson
      if p.tok != tkComma:
        break
      discard p.getTok
    p.eat tkCurlyRi
    if result.len == 1:
      if "$numberDouble" in result and result["$numberDouble"].kind == BsonKindStringUTF8:
        try:
          result = toBson(case result["$numberDouble"].toString
                          of "NaN": NaN
                          of "-Infinity": -Inf
                          of "Infinity": Inf
                          else: parseFloat64(result["$numberDouble"]))
        except ValueError:
          discard
      elif "$binary" in result and result["$binary"].kind == BsonKindDocument:
        try:
          let subdoc = result["$binary"]
          if subdoc.len == 2 and "base64" in subdoc and "subtype" in subdoc:
            var subtype: uint8
            let parsed = parseHex(subdoc["subtype"], subtype)
            if parsed != 0 and parsed == subdoc["subtype"].len:
              let data = base64.decode(subdoc["base64"])
              result = (case subtype.BsonSubtype
                        of BsonSubtypeGeneric: bin(data)
                        of BsonSubtypeFunction: bin(data)  # FIXME in bson
                        of BsonSubtypeBinaryOld: bin(data)
                        of BsonSubtypeUuidOld: bin(data)
                        of BsonSubtypeUuid: bin(data)
                        of BsonSubtypeMd5: cast[ptr MD5Digest](data.cstring)[].toBson()
                        of BsonSubtypeUserDefined: binuser(data)
                        else: raise newException(ValueError, "subtype"))
        except ValueError:
          discard
      elif "$oid" in result and result["$oid"].kind == BsonKindStringUTF8 and result["$oid"].len == 24:
        result = toBson(parseOid(result["$oid"].toString))
      elif "$undefined" in result and result["$undefined"].kind == BsonKindBool and result["$undefined"]:
        result = undefined()
      elif "$date" in result:
        if result["$date"].kind == BsonKindDocument:
          try:
            let subdoc = result["$date"]
            if "$numberLong" in subdoc and subdoc.len == 1 and subdoc["$numberLong"].kind == BsonKindStringUTF8:
              result = timeUTC(subdoc["$numberLong"].parseBiggestInt.fromMilliseconds)
          except ValueError:
            discard
        elif result["$date"].kind == BsonKindStringUTF8:
          var dateStr = result["$date"].toString
          for format in [iso8601MillisecondFormat, iso8601Format]:
            try:
              result = parse(dateStr, format).toTime.toBson
              break
            except TimeParseError:
              discard
      elif "$regularExpression" in result and result.kind == BsonKindDocument:
        let subdoc = result["$regularExpression"]
        if "pattern" in subdoc and "options" in subdoc and subdoc.len == 2 and subdoc["pattern"].kind == BsonKindStringUTF8 and subdoc["options"].kind == BsonKindStringUTF8:
          result = regex(subdoc["pattern"], subdoc["options"])
      elif "$dbPointer" in result and result.kind == BsonKindDocument:
        let subdoc = result["$dbPointer"]
        if (len(subdoc) == 2 and
            "$ref" in subdoc and subdoc["$ref"].kind == BsonKindStringUTF8 and
            "$id" in subdoc and subdoc["$id"].kind == BsonKindDocument and
            "$oid" in subdoc["$id"] and subdoc["$id"]["$oid"].kind == BsonKindStringUTF8):
          result = dbref(subdoc["$ref"], parseOid(subdoc["$id"]["$oid"].toString))
      elif "$code" in result and result["$code"].kind == BsonKindStringUTF8:
        result = js(result["$code"])
      elif "$symbol" in result and result["$symbol"].kind == BsonKindStringUTF8:
        assert false, "symbol BSON type unsupported by bson module"
        # result = js(result["$symbol"])  # FIXME
      elif "$numberInt" in result and result["$numberInt"].kind == BsonKindStringUTF8:
        try:
          result = parseInt(result["$numberInt"].toString).toBson
        except ValueError:
          discard
      elif "$timestamp" in result and result["$timestamp"].kind == BsonKindDocument:
        let subdoc = result["$timestamp"]
        if subdoc.len == 2 and "t" in subdoc and subdoc["t"].kind == BsonKindInt32 and "i" in subdoc and subdoc["i"].kind == BsonKindInt32:
          result = BsonTimestamp(increment: subdoc["i"],
                                 timestamp: subdoc["t"]).toBson
      elif "$numberLong" in result and result["$numberLong"].kind == BsonKindStringUTF8:
        try:
          result = parseBiggestInt(result["$numberLong"]).toBson
        except ValueError:
          discard
      elif "$minKey" in result and result["$minKey"].kind == BsonKindInt64 and result["$minKey"] == 1:
        result = minkey()
      elif "$maxKey" in result and result["$maxKey"].kind == BsonKindInt64 and result["$maxKey"] == 1:
        result = maxkey()
    elif result.len == 2 and "$code" in result and result["$code"].kind == BsonKindStringUTF8 and "$scope" in result and result["$scope"].kind == BsonKindDocument:
      assert false, "JS code with scope unsupported by bson module"
      # result = js(result["$code"].toString, result["$scope"].toString)  # FIXME
  of tkBracketLe:
    result = newBsonArray()
    discard p.getTok
    while p.tok != tkBracketRi:
      result.add(p.parseJsonAsBson)
      if p.tok != tkComma:
        break
      discard p.getTok
    p.eat tkBracketRi
  of tkError, tkCurlyRi, tkBracketRi, tkColon, tkComma, tkEof:
    raise newException(Exception, "invalid json token " & p.a)

proc json2bson*(input: Stream, output: Stream) =
  var parser: JsonParser

  parser.open(input, "")
  try:
    discard parser.getTok
    output.write parser.parseJsonAsBson.bytes
    parser.eat tkEof
  finally:
    parser.close
