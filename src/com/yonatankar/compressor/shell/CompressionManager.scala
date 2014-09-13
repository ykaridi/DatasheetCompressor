package com.yonatankar.compressor.shell

import java.nio.file.Files

import com.yonatankar.compressor.core.benchmarking.Stopwatch
import com.yonatankar.compressor.core.utils.{FileUtil, ZipUtil}
import com.yonatankar.compressor.core.datasetCompression.{DecompressDataset, CompressDataset}

object CompressionManager {
  def compress(inputFile: String, outputFile: String, delimiter: Option[Char] = None) {
    val outputDir = Files.createTempDirectory("DatasheetCompressor").toString

    if (Stopwatch.getState) println("Working directory: " + outputDir)

    CompressDataset.compress(inputFile, outputDir, oDelimiter = delimiter)
    ZipUtil.zip(outputDir, outputFile)

    FileUtil.deleteDirectory(outputDir)
  }

  def decompress(inputFile: String, outputFile: String, delimiter: Option[Char] = None) {
    val outputDir = Files.createTempDirectory("DatasheetDecompressor").toString

    if (Stopwatch.getState) println("Working directory: " + outputDir)

    ZipUtil.unzip(inputFile, outputDir)
    DecompressDataset.decompress(outputDir, outputFile)

    FileUtil.deleteDirectory(outputDir)
  }
}
