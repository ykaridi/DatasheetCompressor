package com.yonatankar.compressor.core.utils

import org.apache.commons.lang3.StringUtils

object Util {
  val emptyNum = BigDecimal(0)
  val emptyExceptional : Map[Int,String] = Map()
  def isNumber(data : String) : Boolean = data.forall(char => char == ',' || char == '.' || char == '-' || char == '-' || char == 'E' || char.isDigit)
  def getNumber(data : String) : BigDecimal = {
    if (data.isEmpty || data == "" || data.equalsIgnoreCase("na") || data.equalsIgnoreCase("n/a")) emptyNum
    else {
      require(isNumber(data), "Not number: " + data)
      BigDecimal(StringUtils.remove(data, ','))
    }
  }

  def adjustBigInt(value: BigInt, size: Int) : Array[Byte] = {
    if (value >= 0) Array.fill(size - value.toByteArray.length)(0.toByte) ++ value.toByteArray
    else Array.fill(size - value.toByteArray.length)(-1.toByte) ++ value.toByteArray
  }

  def toFixedString(value: BigDecimal) : String = {
    if (value.scale > 0 && value * BigDecimal(10).pow(value.scale) % 10 == BigDecimal(0)) toFixedString(value.setScale(value.scale - 1))
    else value.bigDecimal.toPlainString
  }
}
