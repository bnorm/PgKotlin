package com.bnorm.pgkotlin

import kotlin.reflect.*

abstract class PgType<T : Any>(
  val oid: Int,
  val type: KClass<T>
) {
  abstract fun decode(value: ByteArray): T
  abstract fun encode(value: T): ByteArray

  override fun toString(): String {
    return "PgType(oid=$oid, type=$type)"
  }
}
