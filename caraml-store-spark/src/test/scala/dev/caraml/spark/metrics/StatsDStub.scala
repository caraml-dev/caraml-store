package dev.caraml.spark.metrics

import java.net.{DatagramPacket, DatagramSocket, SocketTimeoutException}

import scala.collection.mutable.ArrayBuffer

class StatsDStub {
  val socket = new DatagramSocket()
  socket.setSoTimeout(100)

  def port: Int = socket.getLocalPort

  def receive: Array[String] = {
    val messages: ArrayBuffer[String] = ArrayBuffer()
    var finished                      = false

    do {
      val buf = new Array[Byte](65535)
      val p   = new DatagramPacket(buf, buf.length)
      try {
        socket.receive(p)
      } catch {
        case _: SocketTimeoutException =>
          finished = true
      }
      messages += new String(p.getData, 0, p.getLength)
    } while (!finished)

    messages.toArray
  }

  private val metricLine = """(.+):(.+)\|(.+)#(.+)""".r

  def receivedMetrics: Map[String, Float] = {
    receive
      .flatMap {
        case metricLine(name, value, type_, tags) =>
          Seq(name -> value.toFloat)
        case s: String =>
          Seq()
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
  }
}
