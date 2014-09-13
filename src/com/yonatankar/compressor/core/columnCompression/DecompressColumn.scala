package com.yonatankar.compressor.core.columnCompression

import com.yonatankar.compressor.core.utils.Util
import com.yonatankar.compressor.core.structres.formats.{DateFormat, NumberFormat}
import com.yonatankar.compressor.core.structres.{LazySeq, CompressedColumn}
import Util._


class DecompressColumn {
  def decodeDeltaNumbers(column : CompressedColumn) : Iterator[String] = {
    require(column.header.startsWith("dN"), "Invalid format")

    var numbers = column.objects
    if (column.header.contains("dN>rlS")) numbers = new LazySeq(decodeRunLength(CompressedColumn("rlS",column.objects)))

    val iter = numbers.iterator.map(getNumber).scanLeft(emptyNum)(_ + _)
    iter.next()
    iter.zipWithIndex.map(pair => {
        val index = pair._2
        if (column.exceptional.contains(index)) column.exceptional(index)
        else toFixedString(pair._1)
    })
  }
  def decodeRegularNumbers(column: CompressedColumn) : Iterator[String] = {
    var numbers = column.objects
    if (column.header.contains("rlS")) numbers = new LazySeq(decodeRunLength(CompressedColumn("rlS",column.objects)))

    numbers.iterator.zipWithIndex.map(pair => {
      if (column.exceptional.contains(pair._2)) column.exceptional(pair._2)
      else pair._1
    })
  }
  def decodeNumericalStrings(column : CompressedColumn) : Iterator[String] = {
    require(column.header.startsWith("nS"), "Invalid format")
    try {
      val format = NumberFormat.fromString(column.header.split('>')(1).split('^')(0))
      column.objects.iterator.map(value => format.formatNumber(getNumber(value))).zipWithIndex.map(pair => {
        val index = pair._2
        if (column.exceptional.contains(index)) column.exceptional(index)
        else pair._1
      })
    } catch {
      case _ : Throwable => throw new Exception("Invalid format")
    }
  }
  def decodeDateStrings(column : CompressedColumn) : Iterator[String] = {
    require(column.header.startsWith("dF"), "Invalid format")

    val objects = decodeNumbers(CompressedColumn(column.header.split('*')(1), column.objects, column.exceptional))
    try {
      val format = DateFormat(column.header.split('>')(1).split('*')(0))
      objects.iterator.map(value => format.formatLong(value.toLong))
    } catch {
      case _ : Throwable => throw new Exception("Invalid format")
    }
  }
  def decodeRunLength(column : CompressedColumn) : Iterator[String] = {
    require(column.header.startsWith("rlS"), "Invalid header")

    column.objects.iterator.flatMap(value => {
      val splitted = value.split(",", 2)
      if (splitted.length == 2) List.fill(splitted(0).toInt)(splitted(1))
      else List.fill(splitted(0).toInt)("")
    })
  }

  def decodeNumbers(column : CompressedColumn): LazySeq[String] = {
    val header = column.header
    require(header.contains('N'))
    if (header.startsWith("dN")) new LazySeq(decodeDeltaNumbers(column))
    else if (header.startsWith("nN")) new LazySeq(decodeRegularNumbers(column))
    else new LazySeq(column.objects.iterator.zipWithIndex.map(pair => {
      if (column.exceptional.contains(pair._2)) column.exceptional(pair._2)
      else pair._1
    }))
  }
  def decodeStrings(column : CompressedColumn) : LazySeq[String] = {
    val header = column.header
    require(header.contains("S"), "Invalid format")
    if (header.startsWith("nS")) new LazySeq(decodeNumericalStrings(column))
    else if (header.startsWith("dF")) new LazySeq(decodeDateStrings(column))
    else if(header.startsWith("rlS")) new LazySeq(decodeRunLength(column))
    else column.objects
  }

  def decompress (column : CompressedColumn) : LazySeq[String] = {
    val header = column.header
    var methodName = ""
    if (header.charAt(1) == 'N' || (header.startsWith("[cf]") && header.charAt(5) == 'N')) {
      methodName = "decodeNumbers"
      var nColumn = column
      if (header.startsWith("[cf]")) {
        nColumn = CompressedColumn(header.substring(4), column.objects, column.exceptional)
      }
      val res = decodeNumbers(nColumn)
      if (header.startsWith("[cf]")) {
        res.map(number => {
          try {
            NumberFormat.commaFormat(BigDecimal(number))
          } catch {
            case _: Throwable => number
          }
        })
      } else {
        res
      }
    }
    else if (header.charAt(1) == 'S' || header.charAt(2) == 'S') {
      methodName = "decodeStrings"
      decodeStrings(column)
    }
    else column.objects
  }
}
