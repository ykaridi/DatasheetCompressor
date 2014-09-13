package com.yonatankar.compressor.core.datasetCompression

import java.io._
import java.nio.file.Files

import au.com.bytecode.opencsv.CSVWriter
import com.yonatankar.compressor.core.benchmarking.Stopwatch
import com.yonatankar.compressor.core.utils.FileUtil
import com.yonatankar.compressor.core.utils.Util._
import com.yonatankar.compressor.core.columnCompression.DecompressColumn
import com.yonatankar.compressor.core.structres.{LazySeq, CompressedColumn}

import scala.collection.AbstractIterator

object DecompressDataset {
  def parseExceptionalLine(contents: String) = {
    (contents.split(";")(0).toInt,contents.split(";")(1))
  }
  def decompress(inputDir: String, outputFile: String) {
    Stopwatch.logTime("Decompressing file") {
      val workingDir = Files.createTempDirectory("DatasheetCompressor").toString

      val infoLines = scala.io.Source.fromFile(s"$inputDir/columns/info").getLines()
      val (columns,delimiter) = (infoLines.next().toInt,infoLines.next().charAt(0))
      val headers = new Array[String](columns)
      val decompressor = new DecompressColumn()

      def decompressAtIndex(index: Int): LazySeq[String] = {
        val actualIndex = index + 1
        Stopwatch.logTime(s"[Column $actualIndex] Decompressing process") {
          val headerPath = s"$inputDir/columns/$index/header"
          val columnPath = s"$inputDir/columns/$index/column"

          val lines = scala.io.Source.fromFile(headerPath).getLines()
          val mHeader = lines.next()
          val cHeader = lines.next()
          var eHeader = cHeader
          if (eHeader.startsWith("[cf]")) {
            eHeader = eHeader.substring(4)
          }
          headers(index) = mHeader

          var args: LazySeq[String] = new LazySeq(Iterator(""))
          Stopwatch.logTime(s"@[Column $actualIndex] Column data decompression") {
            if (eHeader.charAt(1) == 'S' || (cHeader.length >= 3 && cHeader.charAt(2) == 'S')) {
              if (eHeader.startsWith("rS")) {
                args = new LazySeq(scala.io.Source.fromFile(columnPath).getLines())
              } else if (eHeader.startsWith("rlS")) {
                args = new LazySeq(readBinaryRunlength(columnPath))
              } else if (eHeader.startsWith("nS")) {
                val nType = eHeader.last
                if (nType == 'i') {
                  args = new LazySeq(readBinaryBigInts(columnPath))
                } else if (nType == 'd') {
                  args = new LazySeq(readBinaryBigDecimals(columnPath))
                }
              }
            } else if (eHeader.charAt(1) == 'N') {
              if (eHeader.startsWith("nN")) {
                val nType = eHeader.last
                if (eHeader.contains("rlS")) {
                  if (nType == 'i') {
                    args = new LazySeq(readBinaryRunlengthBigInts(columnPath))
                  } else if (nType == 'd') {
                    args = new LazySeq(readBinaryRunlengthBigDecimals(columnPath))
                  }
                } else {
                  if (nType == 'i') {
                    args = new LazySeq(readBinaryBigInts(columnPath))
                  } else if (nType == 'd') {
                    args = new LazySeq(readBinaryBigDecimals(columnPath))
                  }
                }
              } else if (eHeader.startsWith("dN")) {
                val nType = eHeader.last
                if (eHeader.contains("rlS")) {
                  if (nType == 'i') {
                    args = new LazySeq(readBinaryRunlengthBigInts(columnPath))
                  } else if (nType == 'd') {
                    args = new LazySeq(readBinaryRunlengthBigDecimals(columnPath))
                  }
                } else {
                  if (nType == 'i') {
                    args = new LazySeq(readBinaryBigInts(columnPath))
                  } else if (nType == 'd') {
                    args = new LazySeq(readBinaryBigDecimals(columnPath))
                  }
                }
              }
            } else if (eHeader.charAt(1) == 'F') {
              args = new LazySeq(scala.io.Source.fromFile(columnPath).getLines())
            }
          }

          val exceptionals = Stopwatch.logTime(s"@[Column $actualIndex] Inserting exceptional values") {
            new File(s"$inputDir/columns/$index/exceptions").listFiles().map(file => {
              val path = file.getPath
              val indexes = readBinaryBigInts(path).map(_.toInt)
              val iter = indexes.scanLeft(0)(_ + _)
              val eVal = if (file.getName == "empty") "" else file.getName
              iter.next()
              iter.map(index => {
                index -> eVal
              }).toMap
            }).fold(emptyExceptional)(_ ++ _)
          }

          decompressor.decompress(CompressedColumn(cHeader, args, exceptionals))
        }
      }
      val indexes = (0 to columns - 1).toList
      val decompressed = indexes.map(decompressAtIndex)

      Stopwatch.logTime("File writing") {
        val decompressedIterators = decompressed.map(_.iterator)
        Stopwatch.logTime("Decompressed columns generation") {
          decompressedIterators.par.zipWithIndex.map(pair => {
            val iter = pair._1
            val index = pair._2
            val writer = new FileWriter(workingDir + "/" + index)
            iter.foreach(value => writer.write(value + "\n"))
            writer.flush()
            writer.close()
          })
        }
        Stopwatch.logTime("Output file generation") {
          val size = scala.io.Source.fromFile(workingDir + "/0").getLines().length
          require((0 to columns - 1).forall(index => scala.io.Source.fromFile(workingDir + "/" + index).getLines().length == size), "File corrupted")
          val decompressedColumns = (0 to columns - 1).map(index => scala.io.Source.fromFile(workingDir + "/" + index).getLines())
          val writer = new FileWriter(outputFile)
          writer.write(headers.mkString(delimiter.toString) + "\n")
          for (i <- 1 to size) {
            writer.write(decompressedColumns.map(iter => {
              var tmp = iter.next()
              if (tmp.contains(delimiter)) tmp = "\"" + tmp + "\""
              tmp
            }).mkString(delimiter.toString) + "\n")
          }
          writer.flush()
          writer.close()
        }
      }
      FileUtil.deleteDirectory(workingDir)
    }
  }

  def readBinaryBigInts(file: String): Iterator[String] = {
    val is = new DataInputStream(new FileInputStream(file))
    val chunkLength = is.readInt()
    val first = is.readLong()
    val iter = new AbstractIterator[BigInt] {
      private val emptyValue = BigInt(0)
      private var nextValue: BigInt = emptyValue
      private var closed = false

      def hasNext: Boolean = {
        if (closed) false
        else {
          if (is.available() <= 0) {
            is.close()
            closed = true
            false
          }
          else {
            val bytes = new Array[Byte](chunkLength)
            is.read(bytes)
            nextValue = BigInt(bytes)
            true
          }
        }
      }

      def next() = {
        val temp = nextValue
        nextValue = emptyValue
        temp
      }
    }
    Iterator(first.toString) ++ iter.map(_.toString())
  }

  def readBinaryBigDecimals(file: String): Iterator[String] = {
    val is = new DataInputStream(new FileInputStream(file))
    val chunkLengths = (is.readInt(), is.readInt())
    val first = BigDecimal(is.readDouble())
    val nFirst = if (first.bigDecimal.toPlainString.endsWith(".0")) first.setScale(0) else first
    val iter = new AbstractIterator[BigDecimal] {
      private val emptyValue = BigDecimal(0)
      private var nextValue: BigDecimal = emptyValue
      private var closed = false

      def hasNext: Boolean = {
        if (closed) false
        else {
          if (is.available() <= 0) {
            is.close()
            closed = true
            false
          }
          else {
            val scaleBytes = new Array[Byte](chunkLengths._2)
            is.read(scaleBytes)
            val scale = BigInt(scaleBytes).toInt
            val bytes = new Array[Byte](chunkLengths._1)
            is.read(bytes)
            nextValue = BigDecimal(BigInt(bytes), scale)
            true
          }
        }
      }

      def next() = {
        val temp = nextValue
        nextValue = emptyValue
        temp
      }
    }
    Iterator(toFixedString(nFirst)) ++ iter.map(toFixedString)
  }

  def readBinaryRunlength(file: String): Iterator[String] = {
    val is = new DataInputStream(new FileInputStream(file))
    val iter = new AbstractIterator[(Int, String)] {
      private val emptyValue = (0, "")
      private var nextValue: (Int, String) = emptyValue
      private var closed = false

      def hasNext: Boolean = {
        if (closed) false
        else {
          if (is.available() <= 0) {
            is.close()
            closed = true
            false
          }
          else {
            val count = is.readInt()
            val rObject = is.readLine().filterNot(_.toInt == 0)
            nextValue = (count, rObject)
            true
          }
        }
      }

      def next() = {
        val temp = nextValue
        nextValue = emptyValue
        temp
      }
    }
    iter.map(pair => {
      val str = pair.toString()
      str.substring(1, str.length - 1)
    })
  }

  def readBinaryRunlengthBigInts(file: String) : Iterator[String] = {
    val is = new DataInputStream(new FileInputStream(file))
    val chunkLengths = (is.readInt(), is.readInt())
    val iter = new AbstractIterator[(BigInt,BigInt)] {
      private val emptyValue = (BigInt(0), BigInt(0))
      private var nextValue: (BigInt,BigInt) = emptyValue
      private var closed = false

      def hasNext: Boolean = {
        if (closed) false
        else {
          if (is.available() <= 0) {
            is.close()
            closed = true
            false
          }
          else {
            val countBytes = new Array[Byte](chunkLengths._1)
            is.read(countBytes)
            val rCount = BigInt(countBytes)
            val bytes = new Array[Byte](chunkLengths._2)
            is.read(bytes)
            nextValue = (rCount, BigInt(bytes))
            true
          }
        }
      }

      def next() = {
        val temp = nextValue
        nextValue = emptyValue
        temp
      }
    }
    iter.map(pair => pair._1 + "," + pair._2)
  }
  def readBinaryRunlengthBigDecimals(file: String) : Iterator[String] = {
    val is = new DataInputStream(new FileInputStream(file))
    val chunkLengths = (is.readInt(), (is.readInt(), is.readInt()))
    val iter = new AbstractIterator[(BigInt,BigDecimal)] {
      private val emptyValue = (BigInt(0), BigDecimal(0))
      private var nextValue: (BigInt,BigDecimal) = emptyValue
      private var closed = false

      def hasNext: Boolean = {
        if (closed) false
        else {
          if (is.available() <= 0) {
            is.close()
            closed = true
            false
          }
          else {
            val countBytes = new Array[Byte](chunkLengths._1)
            is.read(countBytes)
            val rCount = BigInt(countBytes)
            val scaleBytes = new Array[Byte](chunkLengths._2._2)
            is.read(scaleBytes)
            val scale = BigInt(scaleBytes).toInt
            val bytes = new Array[Byte](chunkLengths._2._1)
            is.read(bytes)
            nextValue = (rCount, BigDecimal(BigInt(bytes), scale))
            true
          }
        }
      }

      def next() = {
        val temp = nextValue
        nextValue = emptyValue
        temp
      }
    }
    iter.map(pair => pair._1 + "," + toFixedString(pair._2))
  }
}
