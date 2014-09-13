package com.yonatankar.compressor.shell

import java.io.File

import com.yonatankar.compressor.core.benchmarking.Stopwatch

object CompressionMain {
  private def compress(input: String, output: String) {
    CompressionManager.compress(input, output)
  }
  private def decompress(input: String, output: String) {
    CompressionManager.decompress(input, output)
  }
  private def parse(args: Array[String]) = {
    def following(flag: String) = args.indexOf(flag) match {
      case -1 => None
      case i => Some(args(i + 1))
    }
    def changeExtension(file: String, newExtension: String) = file.split('.').dropRight(1).mkString(".") + newExtension
    val formatString = "Syntax: <compress | decompress> -i <input file> [-o <output dir>] [-benchmark]"

    if (args.length < 2) throw new IllegalArgumentException(formatString)
    if (!(args(0) == "compress") && !(args(0) == "decompress")) throw new IllegalArgumentException(formatString)

    val isCompress = args(0) == "compress"
    val benchmark = args contains "-benchmark"
    val input = following("-i").getOrElse(throw new IllegalArgumentException(formatString))
    val outputdir = following("-o").getOrElse(new File(input).getAbsoluteFile.getParent)

    // Verify files
    if (!new File(input).exists() || !new File(input).isFile) throw new IllegalArgumentException("File doesn't exist!")
    if (!new File(outputdir).exists() || !new File(outputdir).isDirectory) throw new IllegalArgumentException("Output directory doesn't exist!")
    // End files verification

    if (isCompress) {
      val outputFile = outputdir + "/" + input + ".cds"
      Stopwatch.setLogging(benchmark)
      compress(input, outputFile)
      if (benchmark) {
        val origSize = new File(input).length().toDouble
        val finalSize = new File(outputFile).length().toDouble
        println(s"Compression rate: ${((finalSize/origSize)*100).toInt.toString}%")
      }
    }
    else {
      val outputFile = outputdir + "/" + input.split('/').last.split('.').dropRight(1).mkString(".")
      decompress(input, outputFile)
    }
  }

  def main(args: Array[String]) = {
    try {
      parse(args)
    } catch {
      case e : Throwable => println(e.getMessage)
    }
  }
}
