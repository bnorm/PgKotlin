package com.bnorm.pgkotlin.internal

import com.bnorm.pgkotlin.PgType
import com.bnorm.pgkotlin.internal.okio.ByteString
import com.bnorm.pgkotlin.internal.okio.encodeUtf8
import com.bnorm.pgkotlin.internal.okio.ofByteString

internal object PgDefault : PgType<ByteString>(0, ByteString::class) {
  override fun decode(value: ByteString) = value
  override fun encode(value: ByteString) = value
}


internal object PgBool : PgType<Boolean>(16, Boolean::class) {
  val TRUE = ofByteString('t'.toByte())
  val FALSE = ofByteString('f'.toByte())

  override fun decode(value: ByteString) = value == TRUE
  override fun encode(value: Boolean) = if (value) TRUE else FALSE
}

internal object PgByteArray : PgType<ByteString>(17, ByteString::class) {
  override fun decode(value: ByteString) = value
  override fun encode(value: ByteString) = value
}

internal object PgCharacter : PgType<Char>(18, Char::class) {
  override fun decode(value: ByteString): Char {
    require(value.size == 1)
    return value[0].toChar()
  }

  override fun encode(value: Char) = ofByteString(value.toByte())
}

internal object PgName : PgType<ByteString>(19, ByteString::class) {
  override fun decode(value: ByteString) = value
  override fun encode(value: ByteString) = value
}

internal object PgLong : PgType<Long>(20, Long::class) {
  override fun decode(value: ByteString) = value.utf8().toLong()
  override fun encode(value: Long) = encodeUtf8(value.toString())
}

internal object PgShort : PgType<Short>(21, Short::class) {
  override fun decode(value: ByteString) = value.utf8().toShort()
  override fun encode(value: Short) = encodeUtf8(value.toString())
}

//_int2vector 22

internal object PgInteger : PgType<Int>(23, Int::class) {
  override fun decode(value: ByteString) = value.utf8().toInt()
  override fun encode(value: Int) = encodeUtf8(value.toString())
}

//_regproc  24

internal object PgText : PgType<String>(25, String::class) {
  override fun decode(value: ByteString) = value.utf8()
  override fun encode(value: String) = encodeUtf8(value)
}

internal object PgOid : PgType<ByteString>(26, ByteString::class) {
  override fun decode(value: ByteString) = value
  override fun encode(value: ByteString) = value
}

//_tid	27
//_xid	28
//_cid	29
//_oidvector	30

internal object PgJson : PgType<ByteString>(114, ByteString::class) {
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

internal object PgFloat : PgType<Float>(700, Float::class) {
  override fun decode(value: ByteString) = value.utf8().toFloat()
  override fun encode(value: Float) = encodeUtf8(value.toString())
}

internal object PgDouble : PgType<Double>(701, Double::class) {
  override fun decode(value: ByteString) = value.utf8().toDouble()
  override fun encode(value: Double) = encodeUtf8(value.toString())
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
  override fun decode(value: ByteString) = value.utf8()
  override fun encode(value: String) = encodeUtf8(value)
}