package com.yonatankar.compressor.core.utils

object ZipUtil {
  def zip(input: String, output: String) {
    import java.io.{BufferedInputStream, FileInputStream, FileOutputStream, File}
    import java.util.zip.{ZipEntry, ZipOutputStream}

    val bufferSize = 1024 * 1024 // 1 MB

    def listFiles(path: String): Array[File] = {
      new File(path).listFiles().flatMap(file => {
        if (file.isDirectory) if (file.listFiles().length > 0) listFiles(file.getPath) else List(file)
        else List(file)
      }).filterNot(_.getPath.contains("DS_Store"))
    }

    val zip = new ZipOutputStream(new FileOutputStream(output))

    listFiles(input).foreach(file => {
      val name = file.getPath.replace(input,"")
      if (!file.isDirectory) {
        zip.putNextEntry(new ZipEntry(name))
        val in = new BufferedInputStream(new FileInputStream(file.getPath))
        val bytes = Array.ofDim[Byte](bufferSize)
        var currentLength = 0
        while ( {
          currentLength = in.read(bytes); currentLength
        } >= 0) {
          zip.write(bytes, 0, currentLength)
        }
        in.close()
        zip.closeEntry()
      } else {
        zip.putNextEntry(new ZipEntry(name + "/"))
        zip.closeEntry()
      }
    })

    zip.close()
  }
  def unzip(input: String, output: String) {
    import java.util.zip.{ZipEntry, ZipInputStream}
    import java.io.{File, BufferedOutputStream, FileOutputStream, FileInputStream}

    val bufferSize = 1024 * 1024 // 1 MB

    if (new File(output).exists())
      new File(output).mkdir()

    def extractFile(in: ZipInputStream, path: String) {
      new File(path).getParentFile.mkdirs()

      val bos = new BufferedOutputStream(new FileOutputStream(path))
      val bytes = Array.ofDim[Byte](bufferSize)
      var currentLength = 0
      while ({currentLength = in.read(bytes); currentLength} != -1) {
        bos.write(bytes, 0, currentLength)
      }
      bos.flush()
      bos.close()
    }

    val destDir = new File(output)
    if (!destDir.exists())
      destDir.mkdir()

    val zipIn = new ZipInputStream(new FileInputStream(input))
    var entry: ZipEntry = null

    while ({entry = zipIn.getNextEntry; entry} != null) {
      val path = destDir + File.separator + entry.getName
      if (!entry.isDirectory)
        extractFile(zipIn, path)
      else
        new File(path).mkdir()
      zipIn.closeEntry()
    }
    zipIn.close()
  }
}
