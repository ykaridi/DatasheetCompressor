package com.yonatankar.compressor.core.datasetCompression

import java.io.{File, FileOutputStream, DataOutputStream, FileWriter}
import java.nio.file.Files

import au.com.bytecode.opencsv.CSVParser
import com.yonatankar.compressor.core.benchmarking.Stopwatch
import com.yonatankar.compressor.core.utils.{FileUtil, Util}
import com.yonatankar.compressor.core.columnCompression.CompressColumn
import com.yonatankar.compressor.core.structres.LazySeq
import scala.collection.mutable.PriorityQueue
import Util._

object CompressDataset {
  def compress(inputFile: String, outputDir: String, oDelimiter: Option[Char] = None) {
    Stopwatch.logTime("Compressing file") {
      val workingDir = Files.createTempDirectory("DatasheetCompressor").toString
      new File(s"$outputDir/columns").mkdir()

      val delimiter =
        if (oDelimiter.isDefined) oDelimiter.get
        else {
          if (scala.io.Source.fromFile(inputFile).getLines().next().contains(",")) ','
          else if (scala.io.Source.fromFile(inputFile).getLines().next().contains("\t")) '\t'
          else throw new Exception("Couldn't detect delimiter")
        }

      val csvParser = new CSVParser(delimiter)
      val headers = csvParser.parseLine(scala.io.Source.fromFile(inputFile).getLines().next())
      val compressor = new CompressColumn()
      val lineCount = scala.io.Source.fromFile(inputFile).getLines().length - 1

      // Write columns to separate files
      def genColumnFiles(max: Int) {
        val streams = (0 to max).map(i => new FileWriter(workingDir + "/" + i + ".column"))
        scala.io.Source.fromFile(inputFile).getLines().drop(1).foreach(line => {
          csvParser.parseLine(line).zipWithIndex.foreach(pair => {
            streams(pair._2).write(pair._1 + "\n")
          })
        })
        streams.foreach(s => {
          s.flush()
          s.close()
        })
      }
      Stopwatch.logTime("Column files creation") {
        genColumnFiles(headers.length - 1)
      }

      def compressAtIndex(index: Int) = {
        val actualIndex = index + 1
        Stopwatch.logTime(s"[Column $actualIndex] Compression process") {
          val headerPath = s"$outputDir/columns/$index/header"
          val columnPath = s"$outputDir/columns/$index/column"

          val res = Stopwatch.logTime(s"@[Column $actualIndex] Column data compression") {
            compressor.compress(new LazySeq(scala.io.Source.fromFile(workingDir + "/" + index + ".column").getLines()))
          }
          new File(s"$outputDir/columns/$index").mkdir()

          Stopwatch.logTime(s"@[Column $actualIndex] Compressed data writing") {
            if (res.header.charAt(1) == 'S' || (res.header.length >= 3 && res.header.charAt(2) == 'S')) {
              if (res.header.startsWith("rS")) {
                val writer = new FileWriter(columnPath)
                res.objects.foreach(value => {
                  writer.write(value + "\n")
                })
                writer.close()
              } else if (res.header.startsWith("dS")) {
                val numbers = res.objects.map(value => value.split(",").map(_.toInt))
                val writer = new DataOutputStream(new FileOutputStream(columnPath))
                writer.writeInt(numbers.iterator.next().length)
                numbers.foreach(arr => {
                  arr.foreach(value => {
                    writer.writeByte(value)
                  })
                })
                writer.close()
              } else if (res.header.startsWith("rlS")) {
                val writer = new DataOutputStream(new FileOutputStream(columnPath))
                res.objects.foreach(value => {
                  val pair = value.split(",",2)
                  val count = pair(0).toInt
                  val rObject = pair(1)
                  writer.writeInt(count)
                  writer.writeChars(rObject + "\n")
                })
                writer.close()
              } else if (res.header.startsWith("nS")) {
                val nType = res.header.last
                if (nType == 'i') {
                  val asType = res.objects.map(value => BigInt(value))
                  writeBinaryBigInts(asType, columnPath)
                } else if (nType == 'd') {
                  val asType = res.objects.map(value => BigDecimal(value))
                  writeBinaryBigDecimals(asType, columnPath)
                }
              }
            } else if (res.header.charAt(1) == 'N' || (res.header.length > 6 && res.header.charAt(5) == 'N')) {
              val tmpHeader = if (res.header.startsWith("[cf]")) res.header.substring(4) else res.header
              if (tmpHeader.startsWith("nN")) {
                val nType = res.header.last
                if (tmpHeader.contains("rlS")) {
                  if (nType == 'i') {
                    val pairs = res.objects.map(value => {
                      val arr = value.split(",",2)
                      (BigInt(arr(0)), BigInt(arr(1)))
                    })
                    writeBinaryRunlengthBigInts(pairs, columnPath)
                  } else if (nType == 'd') {
                    val pairs = res.objects.map(value => {
                      val arr = value.split(",",2)
                      (BigInt(arr(0)), BigDecimal(arr(1)))
                    })
                    writeBinaryRunlengthBigDecimals(pairs, columnPath)
                  }
                } else {
                  if (nType == 'i') {
                    val asType = res.objects.map(value => BigInt(value))
                    writeBinaryBigInts(asType, columnPath)
                  } else if (nType == 'd') {
                    val asType = res.objects.map(value => BigDecimal(value))
                    writeBinaryBigDecimals(asType, columnPath)
                  }
                }
              } else if (tmpHeader.startsWith("dN")) {
                val nType = res.header.last
                if (tmpHeader.contains("rlS")) {
                  if (nType == 'i') {
                    val pairs = res.objects.map(value => {
                      val arr = value.split(",",2)
                      (BigInt(arr(0)), BigInt(arr(1)))
                    })
                    writeBinaryRunlengthBigInts(pairs, columnPath)
                  } else if (nType == 'd') {
                    val pairs = res.objects.map(value => {
                      val arr = value.split(",",2)
                      (BigInt(arr(0)), BigDecimal(arr(1)))
                    })
                    writeBinaryRunlengthBigDecimals(pairs, columnPath)
                  }
                } else {
                  if (nType == 'i') {
                    val asType = res.objects.map(value => BigInt(value))
                    writeBinaryBigInts(asType, columnPath)
                  } else if (nType == 'd') {
                    val asType = res.objects.map(value => BigDecimal(value))
                    writeBinaryBigDecimals(asType, columnPath)
                  }
                }
              }
            } else if (res.header.charAt(2) == 'F') {
              if (res.header.startsWith("dF")) {
                val writer = new FileWriter(columnPath)
                res.objects.foreach(value => {
                  writer.write(value + "\n")
                })
                writer.close()
              }
            }
          }

          Stopwatch.logTime(s"@[Column $actualIndex] Writing metadata") {
            val HeaderWriter = new FileWriter(headerPath)
            HeaderWriter.write(headers(index) + "\n")
            HeaderWriter.write(res.header + "\n")
            HeaderWriter.close()

            new File(s"$outputDir/columns/$index/exceptions").mkdir()
            res.exceptional.groupBy(_._2).map(pair => {
              val lSeq = new LazySeq({
                val q: PriorityQueue[Int] = PriorityQueue()(Ordering.Int.reverse)
                pair._2.map(_._1).foreach(num => q += num)
                Iterator.fill(q.size)(BigInt(q.dequeue()))
              })
              val sorted = if (lSeq.iterator.length > 1) new LazySeq(Iterator(lSeq.iterator.next()) ++ lSeq.iterator.sliding(2).map(list => list(1) - list(0))) else lSeq
              (pair._1, sorted)
            }).foreach(pair => {
              val cHeader = if (pair._1.length > 0) pair._1 else "empty"
              writeBinaryBigInts(pair._2, s"$outputDir/columns/$index/exceptions/$cHeader")
            })
          }
        }
      }

      val indexes = (0 to headers.length - 1).toList.par
      indexes.foreach(compressAtIndex)

      val infoWriter = new FileWriter(s"$outputDir/columns/info")
      infoWriter.write(headers.length.toString + "\n")
      infoWriter.write(delimiter.toString)
      infoWriter.close()

      FileUtil.deleteDirectory(workingDir)
    }
  }

  def writeBinaryBigInts(objects: LazySeq[BigInt], file: String) {
    val first = objects.iterator.next()
    val newObjects = new LazySeq(objects.iterator.drop(1))
    val maxSize = if (newObjects.iterator.length > 0) newObjects.map(_.toByteArray.length).max else 0

    val finalBytes = newObjects.map(value => {
      adjustBigInt(value,maxSize)
    })
    val writer = new DataOutputStream(new FileOutputStream(file))
    writer.writeInt(maxSize)
    writer.writeLong(first.toLong)
    finalBytes.foreach(_.foreach(value => writer.writeByte(value.toInt)))
    writer.close()
  }
  def writeBinaryBigDecimals(objects: LazySeq[BigDecimal], file: String) {
    val first = objects.iterator.next()
    val newObjects = new LazySeq(objects.iterator.drop(1))
    val asPairs = newObjects.map(value => {
      val fPrecision = value.scale
      ((value * BigDecimal(10).pow(fPrecision)).toBigInt(), BigInt(fPrecision))
    })
    val maxSizes = (asPairs.map(_._1.toByteArray.length).max, asPairs.map(_._2.toByteArray.length).max)

    val finalBytes = asPairs.map(pair => {
      (adjustBigInt(pair._1,maxSizes._1), adjustBigInt(pair._2, maxSizes._2))
    })
    val writer = new DataOutputStream(new FileOutputStream(file))
    writer.writeInt(maxSizes._1)
    writer.writeInt(maxSizes._2)
    writer.writeDouble(first.toDouble)
    finalBytes.foreach(pair => {
      pair._2.foreach(value => writer.writeByte(value.toInt))
      pair._1.foreach(value => writer.writeByte(value.toInt))
    })
    writer.close()
  }

  def writeBinaryRunlengthBigInts(pairs: LazySeq[(BigInt,BigInt)], file: String) {
    val bytePairs = pairs.map(pair => {
      (pair._1.toByteArray, pair._2.toByteArray)
    })
    val maxSizes = (bytePairs.map(_._1.length).max, bytePairs.map(_._2.length).max)
    val finalBytes = pairs.map(pair => {
      (adjustBigInt(pair._1, maxSizes._1), adjustBigInt(pair._2, maxSizes._2))
    })

    val writer = new DataOutputStream(new FileOutputStream(file))
    writer.writeInt(maxSizes._1)
    writer.writeInt(maxSizes._2)
    finalBytes.foreach(pair => {
      pair._1.foreach(cByte => writer.writeByte(cByte.toInt))
      pair._2.foreach(cByte => writer.writeByte(cByte.toInt))
    })
    writer.close()
  }
  def writeBinaryRunlengthBigDecimals(pairs: LazySeq[(BigInt,BigDecimal)], file: String) {
    val nPairs = pairs.map(pair => {
      val fPrecision = pair._2.scale
      (pair._1, ((pair._2 * BigDecimal(10).pow(fPrecision)).toBigInt(), BigInt(fPrecision)))
    })
    val bytePairs = nPairs.map(pair => {
      (pair._1.toByteArray, (pair._2._1.toByteArray, pair._2._2.toByteArray))
    })
    val maxSizes = (bytePairs.map(_._1.length).max, (bytePairs.map(_._2._1.length).max, bytePairs.map(_._2._2.length).max))
    val finalBytes = nPairs.map(pair => {
      (adjustBigInt(pair._1, maxSizes._1), (adjustBigInt(pair._2._1, maxSizes._2._1), adjustBigInt(pair._2._2, maxSizes._2._2)))
    })

    val writer = new DataOutputStream(new FileOutputStream(file))
    writer.writeInt(maxSizes._1)
    writer.writeInt(maxSizes._2._1)
    writer.writeInt(maxSizes._2._2)
    finalBytes.foreach(pair => {
      pair._1.foreach(cByte => writer.writeByte(cByte.toInt))
      pair._2._2.foreach(cByte => writer.writeByte(cByte.toInt))
      pair._2._1.foreach(cByte => writer.writeByte(cByte.toInt))
    })
    writer.close()
  }
}
