package com.yonatankar.compressor.core.structres

import com.yonatankar.compressor.core.utils.Util

case class CompressedColumn(header : String, objects : LazySeq[String], exceptional : Map[Int,String] = Util.emptyExceptional)
