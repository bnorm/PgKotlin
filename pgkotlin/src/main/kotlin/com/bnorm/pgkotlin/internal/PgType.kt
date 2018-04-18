package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.Type
import okio.ByteString
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.util.*
import kotlin.reflect.KClass

private val types = listOf(
  PgBool,
  PgByteArray,
  PgCharacter,
  PgName,
  PgLong,
  PgShort,
  PgInteger,
  PgText,
  PgJson,
  PgFloat,
  PgDouble,
  PgVariableCharacter,
  PgDate,
  PgTime,
  PgTimeStamp,
  PgTimeStampTz,
  PgInterval,
  PgUuid,
  PgJsonBinary
)

private val byType = types.associateBy { it.type }
private val byOid = types.associateBy { it.oid }

internal fun Int.toPgType(): PgType<*> = byOid[this] ?: TODO("unknown = $this")
internal fun <T : Any> KClass<out T>.toPgType(): PgType<T> = (byType[this] as? PgType<T>) ?: TODO()

abstract class PgType<T : Any>(
  override val oid: Int,
  override val type: KClass<T>
) : Type<T>

private object PgNothing : PgType<Nothing>(0, Nothing::class) {
  override fun decode(value: ByteString) = TODO()
  override fun encode(value: Nothing) = TODO()
}

private object PgBool : PgType<Boolean>(16, Boolean::class) {
  val TRUE = ByteString.of('t'.toByte())!!
  val FALSE = ByteString.of('f'.toByte())!!

  override fun decode(value: ByteString) = value == TRUE
  override fun encode(value: Boolean) = if (value) TRUE else FALSE
}

private object PgByteArray : PgType<ByteString>(17, ByteString::class) {
  override fun decode(value: ByteString) = value
  override fun encode(value: ByteString) = value
}

private object PgCharacter : PgType<Char>(18, Char::class) {
  override fun decode(value: ByteString): Char {
    require(value.size() == 1)
    return value.getByte(0).toChar()
  }

  override fun encode(value: Char) = ByteString.of(value.toByte())!!
}

private object PgName : PgType<ByteString>(19, ByteString::class) {
  override fun decode(value: ByteString) = value
  override fun encode(value: ByteString) = value
}

private object PgLong : PgType<Long>(20, Long::class) {
  override fun decode(value: ByteString) = value.utf8().toLong()
  override fun encode(value: Long) = ByteString.encodeUtf8(value.toString())!!
}

private object PgShort : PgType<Short>(21, Short::class) {
  override fun decode(value: ByteString) = value.utf8().toShort()
  override fun encode(value: Short) = ByteString.encodeUtf8(value.toString())!!
}

//_int2vector 22

private object PgInteger : PgType<Int>(23, Int::class) {
  override fun decode(value: ByteString) = value.utf8().toInt()
  override fun encode(value: Int) = ByteString.encodeUtf8(value.toString())!!
}

//_regproc  24

private object PgText : PgType<String>(25, String::class) {
  override fun decode(value: ByteString) = value.utf8()!!
  override fun encode(value: String) = ByteString.encodeUtf8(value)!!
}

//_oid	26
//_tid	27
//_xid	28
//_cid	29
//_oidvector	30

private object PgJson : PgType<ByteString>(114, ByteString::class) {
  override fun decode(value: ByteString) = value
  override fun encode(value: ByteString) = value
}

//_xml	142
//_point	600
//_lseg	601
//_path	602
//_box	603
//_polygon	604
//_line	628
//_cidr	650

private object PgFloat : PgType<Float>(700, Float::class) {
  override fun decode(value: ByteString) = value.utf8().toFloat()
  override fun encode(value: Float) = ByteString.encodeUtf8(value.toString())!!
}

private object PgDouble : PgType<Double>(701, Double::class) {
  override fun decode(value: ByteString) = value.utf8().toDouble()
  override fun encode(value: Double) = ByteString.encodeUtf8(value.toString())!!
}

//_abstime	702
//_reltime	703
//_tinterval	704
//_circle	718
//_macaddr8	774
//_money	790
//_macaddr	829
//_inet	869
//_aclitem	1033
//_bpchar	1042

private object PgVariableCharacter : PgType<String>(1043, String::class) {
  override fun decode(value: ByteString) = value.utf8()!!
  override fun encode(value: String) = ByteString.encodeUtf8(value)!!
}

private object PgDate : PgType<LocalDate>(1082, LocalDate::class) {
  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  override fun decode(value: ByteString) = LocalDate.parse(value.utf8(), formatter)!!
  override fun encode(value: LocalDate) = ByteString.encodeUtf8(value.toString())!!
}

private object PgTime : PgType<LocalTime>(1083, LocalTime::class) {
  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_TIME

  override fun decode(value: ByteString) = LocalTime.parse(value.utf8(), formatter)!!
  override fun encode(value: LocalTime) = ByteString.encodeUtf8(formatter.format(value))!!
}

private object PgTimeStamp : PgType<LocalDateTime>(1114, LocalDateTime::class) {
  val formatter: DateTimeFormatter = DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(PgDate.formatter)
    .appendLiteral(' ')
    .append(PgTime.formatter)
    .parseStrict()
    .toFormatter()

  override fun decode(value: ByteString) = LocalDateTime.parse(value.utf8(), formatter)!!
  override fun encode(value: LocalDateTime) = ByteString.encodeUtf8(formatter.format(value))!!
}

private object PgTimeStampTz : PgType<ZonedDateTime>(1184, ZonedDateTime::class) {
  val formatter: DateTimeFormatter = DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(PgTimeStamp.formatter)
    .appendOffset("+HH", "+00")
    .parseStrict()
    .toFormatter()

  override fun decode(value: ByteString) = ZonedDateTime.parse(value.utf8(), formatter)!!
  override fun encode(value: ZonedDateTime) = ByteString.encodeUtf8(formatter.format(value))!!
}

private object PgInterval : PgType<Duration>(1186, Duration::class) {
  override fun decode(value: ByteString) = Duration.parse(value.utf8())!! // Period?
  override fun encode(value: Duration) = ByteString.encodeUtf8(value.toString())!!
}

//_timetz	1266
//_bit	1560
//_varbit	1562
//_numeric	1700
//_refcursor	1790
//_regprocedure	2202
//_regoper	2203
//_regoperator	2204
//_regclass	2205
//_regtype	2206
//_record	2249
//_cstring	2275

private object PgUuid : PgType<UUID>(2950, UUID::class) {
  override fun decode(value: ByteString) = UUID.fromString(value.utf8())!!
  override fun encode(value: UUID) = ByteString.encodeUtf8(value.toString())!!
}

//_txid_snapshot	2970
//_pg_lsn	3220
//_tsvector	3614
//_tsquery	3615
//_gtsvector	3642
//_regconfig	3734
//_regdictionary	3769

private object PgJsonBinary : PgType<ByteString>(3802, ByteString::class) {
  override fun decode(value: ByteString) = value
  override fun encode(value: ByteString) = value
}

//_int4range	3904
//_numrange	3906
//_tsrange	3908
//_tstzrange	3910
//_daterange	3912
//_int8range	3926
//_regnamespace	4089
//_regrole	4096

// ===== ARRAYS ===== //

//xml	143	0
//json	199	0
//line	629	701
//cidr	651	0
//circle	719	0
//macaddr8	775	0
//money	791	0
//bool	1000	0
//bytea	1001	0
//char	1002	0
//name	1003	18
//int2	1005	0
//int2vector	1006	21
//int4	1007	0
//regproc	1008	0
//text	1009	0
//tid	1010	0
//xid	1011	0
//cid	1012	0
//oidvector	1013	26
//bpchar	1014	0
//varchar	1015	0
//int8	1016	0
//point	1017	701
//lseg	1018	600
//path	1019	0
//box	1020	600
//float4	1021	0
//float8	1022	0
//abstime	1023	0
//reltime	1024	0
//tinterval	1025	0
//polygon	1027	0
//oid	1028	0
//aclitem	1034	0
//macaddr	1040	0
//inet	1041	0
//timestamp	1115	0
//date	1182	0
//time	1183	0
//timestamptz	1185	0
//interval	1187	0
//numeric	1231	0
//cstring	1263	0
//timetz	1270	0
//bit	1561	0
//varbit	1563	0
//refcursor	2201	0
//regprocedure	2207	0
//regoper	2208	0
//regoperator	2209	0
//regclass	2210	0
//regtype	2211	0
//record	2287	0
//txid_snapshot	2949	0
//uuid	2951	0
//pg_lsn	3221	0
//tsvector	3643	0
//gtsvector	3644	0
//tsquery	3645	0
//regconfig	3735	0
//regdictionary	3770	0
//jsonb	3807	0
//int4range	3905	0
//numrange	3907	0
//tsrange	3909	0
//tstzrange	3911	0
//daterange	3913	0
//int8range	3927	0
//regnamespace	4090	0
//regrole	4097	0
