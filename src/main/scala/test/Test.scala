package test

import java.io.PrintWriter
import java.net.Socket

object Test {
  def main(args: Array[String]): Unit = {
    val socket = new Socket("localhost", 9999)
    println("success")
    val out = new PrintWriter(socket.getOutputStream)
    while (true) {
      out.println("This is an message....")
      out.flush()
      Thread.sleep(1000)
    }
  }
}
