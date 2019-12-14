import parseutils
import times


const
  iso8601DateFormat* = initTimeFormat("yyyy-MM-dd")
  iso8601NaiveFormat* = initTimeFormat("yyyy-MM-dd'T'HH:mm:ss")
  iso8601Format* = initTimeFormat("yyyy-MM-dd'T'HH:mm:sszzz")
  iso8601MillisecondFormat* = initTimeFormat("yyyy-MM-dd'T'HH:mm:ss'.'fffzzz")


func fromMilliseconds*(since1970: int64): Time =
  initTime(since1970 div 1000, since1970 mod 1000 * 1000000)


func toIso8601*(dt: DateTime): string =
  dt.format(if dt.nanosecond div 1000000 mod 1000 != 0: iso8601MillisecondFormat else: iso8601Format)


proc toIso8601*(t: Time): string =
  t.utc.toIso8601


proc toMilliseconds*(t: Time): int64 =
  t.toUnix * 1000 + t.nanosecond div 1000000


func isNaN*(x: float): bool {.importc: "isnan", header: "<math.h>".}


func parseFloat64*(s: string): float64 {.gcsafe, raises: [ValueError].} =
  var parsedValue: BiggestFloat
  if s.len == 0 or parseBiggestFloat(s, parsedValue, 0) != s.len:
    raise newException(ValueError, "invalid float64: " & s)
  result = parsedValue.float64


proc toExtJsonString*(t: Time): string =
  let dt = t.utc
  if 1970 <= dt.year and dt.year <= 9999:
    dt.toIso8601
  else:
    ""
