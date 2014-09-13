package com.yonatankar.compressor.core.benchmarking

class Stopwatch {

  private var startTime = -1L
  private var stopTime = -1L
  private var running = false

  def start(): Stopwatch = {
    startTime = System.currentTimeMillis()
    running = true
    this
  }

  def stop(): Stopwatch = {
    stopTime = System.currentTimeMillis()
    running = false
    this
  }

  def isRunning(): Boolean = running

  def getElapsedTime() = {
    if (startTime == -1) {
      0
    }
    if (running) {
      System.currentTimeMillis() - startTime
    }
    else {
      stopTime - startTime
    }
  }

  def reset() {
    startTime = -1
    stopTime = -1
    running = false
  }
}

object Stopwatch {
  private var logState = true
  def setLogging(state: Boolean) { logState = state }
  def getState = logState

  def logTime[T](key: String, logStart: Boolean = true, logEnd: Boolean = true)(f: => T) = {
    if (logStart) {
      if (logState)
        println(s"executing $key")
    }
    val start = System.currentTimeMillis
    val res = f
    val duration = (System.currentTimeMillis - start).toDouble / 1000
    if (logEnd) {
      if (logState)
        println(s"$key finished in $duration seconds")
    }
    res
  }
}
