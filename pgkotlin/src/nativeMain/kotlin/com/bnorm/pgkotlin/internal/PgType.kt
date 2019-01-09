package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*
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
  PgVariableCharacter
)

private val byType: Map<KClass<*>, PgType<*>> = types.associateBy { it.type }
private val byOid: Map<Int, PgType<*>> = types.associateBy { it.oid }

internal fun Int.toPgType(): PgType<*> = byOid[this] ?: PgDefault
internal fun Int.pgDecode(value: ByteArray): Any = toPgType().decode(value)
internal fun <T : Any> KClass<out T>.toPgType(): PgType<T> = (byType[this] as? PgType<T>) ?: TODO("type=$this")
internal fun Any?.pgEncode(): ByteArray? =
  if (this == null) null else this::class.toPgType().encode(this)
