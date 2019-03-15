package io.github.yuemenglong.speak.s00

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.{ServerSocket, Socket, SocketException}

class Rpc(port: Int, fn: Object => Object = null) {
  @volatile var running = true
  val server = new ServerSocket(port)

  def start(): Rpc = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (running) {
          val socket = try {
            server.accept()
          } catch {
            case _: SocketException =>
              running = false
              null
          }
          if (socket != null) {
            try {
              val req = Rpc.readObject(socket)
              val res = fn(req)
              Rpc.writeObject(socket, res)
            } finally {
              socket.close()
            }
          }
        }
      }
    }).start()
    this
  }

  def stop(): Unit = {
    running = false
    server.close()
  }
}

case class RpcOk()

object Rpc {
  val HEADER_LEN: Int = 8

  protected def readObject(socket: Socket): Object = {
    val is = socket.getInputStream
    val lbs = new Array[Byte](HEADER_LEN)
    is.read(lbs)
    val len = new String(lbs).toInt
    val data = new Array[Byte](len)
    is.read(data)
    val bis = new ByteArrayInputStream(data)
    val ois = new ObjectInputStream(bis)
    ois.readObject()
  }

  protected def writeObject(socket: Socket, obj: Object): Unit = {
    val os = socket.getOutputStream
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(obj)
    val data = bos.toByteArray
    val len = data.length
    val lbs = f"${len}%08d".getBytes()
    os.write(lbs)
    os.write(data)
  }

  def call(host: String, port: Int, req: Object): Object = {
    val socket = new Socket(host, port)
    writeObject(socket, req)
    val res = readObject(socket)
    socket.close()
    res
  }

  //  def main(args: Array[String]): Unit = {
  //    case class Req()
  //    case class Res()
  //
  //    val rpc = new Rpc(12345, {
  //      case Req() => Res()
  //    }).start()
  //
  //    val res = Rpc.call("localhost", 12345, Req())
  //    println(res)
  //    rpc.stop()
  //  }
}
