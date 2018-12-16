package io.github.yuemenglong.speak

import java.io._
import java.net.{InetSocketAddress, ServerSocket, Socket}

/**
  * Created by <yuemenglong@126.com> on 2018/12/15.
  */
object Rpc {
  def send(req: Object, host: String, port: Int): Object = {
    val socket = new Socket(host, port)
    val bs = new ByteArrayOutputStream()
    val os = new ObjectOutputStream(bs)
    os.writeObject(req)
    val reqData = bs.toByteArray
    socket.getOutputStream.write(f"${reqData.length}%08d".getBytes())
    socket.getOutputStream.write(reqData)
    val lenbs = new Array[Byte](8)
    socket.getInputStream.read(lenbs)
    val len = new String(lenbs).toInt
    val resData = new Array[Byte](len)
    socket.getInputStream.read(resData)
    val is = new ObjectInputStream(new ByteArrayInputStream(resData))
    val res = is.readObject()
    socket.close()
    res
  }
}

class Rpc(port: Int, fn: Object => Object) extends Thread {
  override def run(): Unit = {
    val server = new ServerSocket(port)
    while (true) {
      val socket = server.accept()
      val lenbs = new Array[Byte](8)
      socket.getInputStream.read(lenbs)
      val len = new String(lenbs).toInt
      val reqData = new Array[Byte](len)
      socket.getInputStream.read(reqData)
      val is = new ObjectInputStream(new ByteArrayInputStream(reqData))
      val req = is.readObject()
      val res = fn(req)
      val bs = new ByteArrayOutputStream()
      val os = new ObjectOutputStream(bs)
      os.writeObject(res)
      val resData = bs.toByteArray
      socket.getOutputStream.write(f"${resData.length}%08d".getBytes())
      socket.getOutputStream.write(resData)
      socket.close()
    }
  }
}
