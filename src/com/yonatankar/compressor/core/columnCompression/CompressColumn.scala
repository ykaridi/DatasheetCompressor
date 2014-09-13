package com.yonatankar.compressor.core.columnCompression

import com.yonatankar.compressor.core.utils.Util
import com.yonatankar.compressor.core.structres.formats.{DateFormat, NumberFormat}
import com.yonatankar.compressor.core.structres.{CompressedColumn, LazySeq}
import Util._

import scala.collection.AbstractIterator

class CompressColumn {
  def encodeDeltaNumbers(column: LazySeq[String]): CompressedColumn = {
    val exceptional = column.iterator.zipWithIndex.filterNot(pair => isNumber(pair._1) && pair._1.length > 0).map(_.swap).toMap
    val res = new LazySeq((Iterator(getNumber(column.iterator.next())) ++ column.iterator.map(getNumber).sliding(2).map(list => list(1) - list(0))).map(_.toString()))
    CompressedColumn("dN", res, exceptional)
  }

  def encodeNumericalStrings(column: LazySeq[String]): CompressedColumn = {
    try {
      val exceptional = column.iterator.zipWithIndex.filterNot(pair => pair._1.length > 0).map(_.swap).toMap

      val format = NumberFormat.getFormat(column)
      val objects = new LazySeq(column.iterator.map(value => format.formatString(value)))
      var nType = '\0'
      if (objects.forall(_%1==0)) nType = 'i'
      else nType = 'd'
      CompressedColumn(s"nS>${format.toString()}^$nType", objects.map(_.toString()), exceptional)
    } catch {
      case _: Throwable => throw new Exception("Numbers don't have format")
    }
  }
  def encodeDateStrings(column: LazySeq[String]) : CompressedColumn = {
    try {
      val format = DateFormat.getFormat(column)
      val objects = new LazySeq(column.iterator.map(value => format.formatString(value)))
      val fObjects = encodeNumbers(objects.map(value => value.toString))
      CompressedColumn(s"dF>${format.toString()}*${fObjects.header}", fObjects.objects)
    } catch {
      case _: Throwable => throw new Exception("Dates don't have format")
    }
  }

  def encodeRunlength(objects: LazySeq[String]): CompressedColumn = {
    def iter = new AbstractIterator[(String, Int)] {
      private val origIterator = objects.iterator
      private var current: String = ""
      private var currentCounter: Int = 0
      private var nextValue: (String, Int) = ("", 0)

      def hasNext: Boolean = {
        if (nextValue._2 > 0) true
        else {
          if (origIterator.hasNext) {
            if (currentCounter < 1) {
              current = origIterator.next()
              currentCounter = 1
            }
            var iterObject: String = ""
            while (origIterator.hasNext && {
              iterObject = origIterator.next(); iterObject == current
            }) {
              currentCounter += 1
            }
            nextValue = (current, currentCounter)
            if (iterObject != current) {
              current = iterObject; currentCounter = 1
            }
            else currentCounter = 0
            true
          } else {
            if (currentCounter > 0) {
              nextValue = (current, 1)
              currentCounter = 0
              true
            } else false
          }
        }
      }

      def next() = {
        val temp = nextValue
        nextValue = ("", 0)
        temp
      }
    }

    CompressedColumn("rlS", new LazySeq(iter.map(value => {
      val strValue = value.swap.toString()
      strValue.substring(1, strValue.length - 1)
    })))
  }

  def encodeNumbers(column: LazySeq[String]): CompressedColumn = {
    val delta = encodeDeltaNumbers(column)
    val deltaLength = delta.objects.iterator.foldLeft(0)((agg, next) => next.length + agg)
    val normalLength = column.iterator.foldLeft(0)((agg, next) => next.length + agg)
    if (deltaLength <= normalLength) {
      var nType = '\0'
      if (delta.objects.forall(value => getNumber(value)%1==emptyNum)) nType = 'i'
      else nType = 'd'
      val nLength = delta.objects.iterator.foldLeft(0)((agg, next) => next.length + agg)
      val runlength = encodeRunlength(delta.objects)
      val rLength = runlength.objects.iterator.foldLeft(0)((agg, next) => next.count(_ != ',') + agg)
      if (rLength < nLength) CompressedColumn(s"dN>rlS^$nType", runlength.objects, delta.exceptional)
      else CompressedColumn(s"dN^$nType", delta.objects, delta.exceptional)
    } else {
      var nType = '\0'
      if (column.forall(value => getNumber(value)%1==emptyNum)) nType = 'i'
      else nType = 'd'

      val exceptional = column.iterator.zipWithIndex.filterNot(pair => isNumber(pair._1) && pair._1.length > 0).map(_.swap).toMap
      val runlength = encodeRunlength(column.map(getNumber).map(_.toString()))
      val rLength = runlength.objects.iterator.foldLeft(0)((agg, next) => next.count(_ != ',') + agg)
      if (rLength < normalLength) CompressedColumn(s"nN>rlS^$nType", runlength.objects, exceptional)
      else CompressedColumn(s"nN&$nType", column.map(value => getNumber(value).toString()), exceptional)
    }
  }

  def encodeStrings(column: LazySeq[String]): CompressedColumn = {
    try {
      val filtered = column.filter(_.length > 0)
      if (NumberFormat.hasFormat(filtered)) encodeNumericalStrings(column)
      else if (DateFormat.hasFormat(filtered)) encodeDateStrings(column)
      else {
        val regular = column.iterator.foldLeft(0)((agg, next) => next.toString.length + agg)
        val rl = encodeRunlength(column)
        val rlLength = rl.objects.iterator.foldLeft(0)((agg, next) => next.toString.count(_ != ',') + agg)
        if (rlLength < regular) rl
        else CompressedColumn("rS", column)
      }
    } catch {
      case _: Throwable => CompressedColumn("rS", column)
    }
  }

  def compress(column: LazySeq[String]): CompressedColumn = {
    val filtered = column.filter(_.toString.nonEmpty)

    val first = filtered.iterator.next()
    var methodName = ""
    try {
      if (isNumber(first) && column.forall(value => value.equalsIgnoreCase("na") || value.equalsIgnoreCase("n/a") || isNumber(value))) {
        methodName = "encodeNumbers"
        var res = encodeNumbers(column)
        if (filtered.forall(number => NumberFormat.isCommaFormatted(number))) {
          res = CompressedColumn("[cf]" + res.header, res.objects, res.exceptional)
        }
        res
      }
      else {
        methodName = "encodeStrings"
        encodeStrings(new LazySeq(column.iterator))
      }
    } catch {
      case _ : Throwable => {
        methodName = "encodeStrings"
        encodeStrings(new LazySeq(column.iterator))
      }
    }
  }
}
