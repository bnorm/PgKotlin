package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.*
import kotlinx.io.core.*

internal object PgDefault : PgType<ByteArray>(0, ByteArray::class) {
  override fun decode(value: ByteArray) = value
  override fun encode(value: ByteArray) = value
}


internal object PgBool : PgType<Boolean>(16, Boolean::class) {
  val TRUE = ByteArray(1) { 't'.toByte() }
  val FALSE = ByteArray(1) { 'f'.toByte() }

  override fun decode(value: ByteArray) = value.contentEquals(TRUE)
  override fun encode(value: Boolean) = if (value) TRUE else FALSE
}

internal object PgByteArray : PgType<ByteArray>(17, ByteArray::class) {
  override fun decode(value: ByteArray) = value
  override fun encode(value: ByteArray) = value
}

internal object PgCharacter : PgType<Char>(18, Char::class) {
  override fun decode(value: ByteArray): Char {
    require(value.size == 1)
    return value[0].toChar()
  }

  override fun encode(value: Char) = ByteArray(1) { value.toByte() }
}

internal object PgName : PgType<ByteArray>(19, ByteArray::class) {
  override fun decode(value: ByteArray) = value
  override fun encode(value: ByteArray) = value
}

internal object PgLong : PgType<Long>(20, Long::class) {
  override fun decode(value: ByteArray) = String(value).toLong()
  override fun encode(value: Long) = value.toString().toByteArray()
}

internal object PgShort : PgType<Short>(21, Short::class) {
  override fun decode(value: ByteArray) = String(value).toShort()
  override fun encode(value: Short) = value.toString().toByteArray()
}

//_int2vector 22

internal object PgInteger : PgType<Int>(23, Int::class) {
  override fun decode(value: ByteArray) = String(value).toInt()
  override fun encode(value: Int) = value.toString().toByteArray()
}

//_regproc  24

internal object PgText : PgType<String>(25, String::class) {
  override fun decode(value: ByteArray) = String(value)
  override fun encode(value: String) = value.toByteArray()
}

internal object PgOid : PgType<ByteArray>(26, ByteArray::class) {
  override fun decode(value: ByteArray) = value
  override fun encode(value: ByteArray) = value
}

//_tid	27
//_xid	28
//_cid	29
//_oidvector	30

internal object PgJson : PgType<ByteArray>(114, ByteArray::class) {
  override fun decode(value: ByteArray) = value
  override fun encode(value: ByteArray) = value
}

//_xml	142
//_point	600
//_lseg	601
//_path	602
//_box	603
//_polygon	604
//_line	628
//_cidr	650

internal object PgFloat : PgType<Float>(700, Float::class) {
  override fun decode(value: ByteArray) = String(value).toFloat()
  override fun encode(value: Float) = value.toString().toByteArray()
}

internal object PgDouble : PgType<Double>(701, Double::class) {
  override fun decode(value: ByteArray) = String(value).toDouble()
  override fun encode(value: Double) = value.toString().toByteArray()
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

internal object PgVariableCharacter : PgType<String>(1043, String::class) {
  override fun decode(value: ByteArray) = String(value)
  override fun encode(value: String) = value.toByteArray()
}
