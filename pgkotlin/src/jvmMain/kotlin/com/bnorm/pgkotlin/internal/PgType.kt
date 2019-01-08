package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*
import kotlinx.io.core.*
import java.time.*
import java.time.format.*
import java.util.*
import kotlin.reflect.*

private val types = listOf(
  PgBool,
  PgByteArray,
  PgCharacter,
  PgName,
  PgLong,
  PgShort,
  PgInteger,
  PgText,
  PgOid,
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

private val byType: Map<KClass<*>, PgType<*>> = types.associateBy { it.type }
private val byOid: Map<Int, PgType<*>> = types.associateBy { it.oid }

internal fun Int.toPgType(): PgType<*> = byOid[this] ?: PgDefault
internal fun Int.pgDecode(value: ByteArray): Any = toPgType().decode(value)
internal fun <T : Any> KClass<out T>.toPgType(): PgType<T> = (byType[this] as? PgType<T>) ?: TODO("type=$this")
internal fun Any?.pgEncode(): ByteArray? =
  if (this == null) null else this::class.toPgType().encode(this)


private object PgDate : PgType<LocalDate>(1082, LocalDate::class) {
  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  override fun decode(value: ByteArray) = LocalDate.parse(value.toString(Charsets.UTF_8), formatter)!!
  override fun encode(value: LocalDate): ByteArray = formatter.format(value).toByteArray()
}

private object PgTime : PgType<LocalTime>(1083, LocalTime::class) {
  val formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_TIME

  override fun decode(value: ByteArray) = LocalTime.parse(value.toString(Charsets.UTF_8), formatter)!!
  override fun encode(value: LocalTime) = formatter.format(value).toByteArray()
}

private object PgTimeStamp : PgType<LocalDateTime>(1114, LocalDateTime::class) {
  val formatter: DateTimeFormatter = DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(PgDate.formatter)
    .appendLiteral(' ')
    .append(PgTime.formatter)
    .parseStrict()
    .toFormatter()

  override fun decode(value: ByteArray) = LocalDateTime.parse(value.toString(Charsets.UTF_8), formatter)!!
  override fun encode(value: LocalDateTime) = formatter.format(value).toByteArray()
}

private object PgTimeStampTz : PgType<ZonedDateTime>(1184, ZonedDateTime::class) {
  val formatter: DateTimeFormatter = DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(PgTimeStamp.formatter)
    .appendOffset("+HH", "+00")
    .parseStrict()
    .toFormatter()

  override fun decode(value: ByteArray) = ZonedDateTime.parse(value.toString(Charsets.UTF_8), formatter)!!
  override fun encode(value: ZonedDateTime) = formatter.format(value).toByteArray()
}

private object PgInterval : PgType<Duration>(1186, Duration::class) {
  override fun decode(value: ByteArray) = Duration.parse(value.toString(Charsets.UTF_8))!! // Period?
  override fun encode(value: Duration) = value.toString().toByteArray()
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
  override fun decode(value: ByteArray) = UUID.fromString(value.toString(Charsets.UTF_8))!!
  override fun encode(value: UUID) = value.toString().toByteArray()
}

//_txid_snapshot	2970
//_pg_lsn	3220
//_tsvector	3614
//_tsquery	3615
//_gtsvector	3642
//_regconfig	3734
//_regdictionary	3769

private object PgJsonBinary : PgType<String>(3802, String::class) {
  override fun decode(value: ByteArray) = value.toString(Charsets.UTF_8)
  override fun encode(value: String) = value.toByteArray()
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
