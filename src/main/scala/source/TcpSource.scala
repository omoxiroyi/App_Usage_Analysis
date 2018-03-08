package source

import java.io.PrintWriter
import java.net.ServerSocket

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TcpSource extends App {
  val serverSocket = new ServerSocket(9999)
  Stream.from(0).foreach { cnt =>
    val socket = serverSocket.accept

    println(s"A client connected as index $cnt")
    Future {
      val out = new PrintWriter(socket.getOutputStream)
      while (true) {

        out.println("This is a test message")
        out.flush()
        Thread.sleep(1000)
      }
    }
  }
}