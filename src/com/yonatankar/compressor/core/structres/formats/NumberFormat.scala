package com.yonatankar.compressor.core.structres.formats

import com.yonatankar.compressor.core.utils.Util
import com.yonatankar.compressor.core.structres.LazySeq
import Util._

case class NumberFormat(prefix:Option[String],suffix:Option[String]) {
  def formatNumber(number : BigDecimal) : String = {
    var formatted = number.toString()
    if (prefix.isDefined) formatted = prefix.get + formatted
    if (suffix.isDefined) formatted = formatted + suffix.get
    formatted
  }
  def formatString(string : String) : BigDecimal = {
    if (string.isEmpty) BigDecimal(0)
    else {
      var fixed = string
      if (prefix.isDefined) fixed = fixed.substring(prefix.get.length)
      if (suffix.isDefined) fixed = fixed.substring(0, fixed.length - suffix.get.length)
      require(isNumber(fixed), "Number invalid")
      getNumber(fixed)
    }
  }
  def isInFormat(string : String) : Boolean = {
    var fixed = string
    if (prefix.isDefined) fixed = fixed.substring(prefix.get.length)
    if (suffix.isDefined) fixed = fixed.substring(0, fixed.length - suffix.get.length)
    if(isNumber(fixed)) true
    else false
  }

  override def toString() = {
    if (prefix.isDefined && suffix.isDefined) prefix.get + "," + suffix.get
    else if (prefix.isDefined) prefix.get + ","
    else if (suffix.isDefined) "," + suffix.get
    else ","
  }
}
object NumberFormat {
  def getFormat(numbers : LazySeq[String]) : NumberFormat = {
    val filtered = numbers.filter(_.length > 0)

    var prefixLength = 0
    var continueLoop = true
    while (continueLoop) {
      prefixLength += 1
      val iter = filtered.iterator.map(_.substring(0,prefixLength))
      val first = iter.next()
      continueLoop = iter.forall(_ == first)
    }
    prefixLength -= 1

    continueLoop = true
    var suffixLength = 0
    while (continueLoop) {
      suffixLength += 1
      val iter = filtered.iterator.map(value => value.substring(value.length - suffixLength))
      val first = iter.next()
      continueLoop = iter.forall(_ == first)
    }
    suffixLength -= 1

    val sampleObject = numbers.iterator.next()
    val cFormat = NumberFormat(Some(sampleObject.substring(0, prefixLength)),Some(sampleObject.substring(sampleObject.length - suffixLength)))
    require(numbers.iterator.filter(_.length > 0).forall(cFormat.isInFormat), "Not all numbers fit")
    cFormat
  }
  def hasFormat(numbers : LazySeq[String]) : Boolean = {
    try {
      NumberFormat.getFormat(numbers)
      true
    } catch {
      case _ : Throwable => false
    }
  }

  def fromString(format : String) : NumberFormat = {
    require(format.contains(','), "Invalid format")
    val parts = format.split(',')
    require(parts.length == 2, "Invalid format")
    val prefix = parts(0)
    val suffix = parts(1)
    if (prefix.length > 0 && suffix.length > 0) NumberFormat(Some(prefix),Some(suffix))
    else if (prefix.length > 0) NumberFormat(Some(prefix),None)
    else if (suffix.length > 0) NumberFormat(None,Some(suffix))
    else throw new Exception("Invalid format")
  }

  def isCommaFormatted(number: String) : Boolean = number.split('.')(0).grouped(4).forall(digits => digits.length < 4 || digits.count(_ == ',') == 1) && number.split('.')(0).length >= 4
  def commaFormat(number: BigInt) : String = number.toString().reverse.grouped(3).mkString(",").reverse
  def commaFormat(number: BigDecimal) : String = if (number == BigDecimal(0)) "0" else commaFormat(BigInt(number.toString().split('.')(0))) + "." + number.toString().split('.')(1)
}
