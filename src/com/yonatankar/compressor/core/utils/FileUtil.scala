package com.yonatankar.compressor.core.utils

import java.io.File

object FileUtil {
  def deleteDirectory(path: String) {
    new File(path).listFiles().foreach(file => {
      if (file.isDirectory) deleteDirectory(file.getPath)
      else file.delete()
    })
    new File(path).delete()
  }
}
