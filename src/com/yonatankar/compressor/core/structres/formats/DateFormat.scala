package com.yonatankar.compressor.core.structres.formats

import com.yonatankar.compressor.core.structres.LazySeq
import java.text.SimpleDateFormat
import java.util.Date

case class DateFormat(formatType : String) {
  def formatLong(longRep: Long) : String = {
    val date = new Date(longRep)
    val format = new SimpleDateFormat(formatType)
    format.format(date)
  }
  def formatString(string : String) : Long = {
    require(isInFormat(string), "String not in format")
    val sFormat = new SimpleDateFormat(formatType)
    sFormat.parse(string).getTime
  }
  def isInFormat(string : String) : Boolean = {
    try {
      val sFormat = new SimpleDateFormat(formatType)
      sFormat.parse(string)
      true
    } catch {
      case _ : Throwable => false
    }
  }

  override def toString() = {
    formatType
  }
}
object DateFormat {
  val formats = List("yyyy-MM-dd", "yyyy/MM/dd", "yyyy:MM:dd", "MM/dd/yyyy", "MM-dd-yyyy", "MM:dd:yyyy")

  def isDate(data : String) : Boolean = {
    formats.exists(format => {
      val sFormat = new SimpleDateFormat(format)
      try {
        sFormat.parse(data)
        true
      } catch {
        case _ : Throwable => false
      }
    })
  }
  def getDateFormat(data : String) : String = {
    require(isDate(data), "input isnt a date")
    formats.filter(format => {
      val sFormat = new SimpleDateFormat(format)
      try {
        sFormat.parse(data)
        true
      } catch {
        case _ : Throwable => false
      }
    })(0)
  }

  def getFormat(numbers : LazySeq[String]) : DateFormat = {
    require(numbers.iterator.forall(isDate), "Not all dates")
    val iter = numbers.iterator.map(value => getDateFormat(value))
    val first = iter.next()
    require(iter.forall(_.equals(first)), "Dates are in different formats")
    require(first != "", "No format found")
    DateFormat(first)
  }
  def hasFormat(numbers : LazySeq[String]) : Boolean = {
    try {
      DateFormat.getFormat(numbers)
      true
    } catch {
      case _ : Throwable => false
    }
  }
}