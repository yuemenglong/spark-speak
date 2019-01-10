package io.github.yuemenglong.speak.s03

import java.net.ServerSocket

object Util {

  def testRandPort(): Int = {
    val port = (Math.random() * 50000).toInt + 10000
    val p = testPort(port, 65535 - port)
    p
  }

  def testPort(port: Int, range: Int): Int = {
    port.until(port + range).find(p => {
      try {
        new ServerSocket(p).close()
        true
      } catch {
        case _: Throwable => false
      }
    }).get
  }

}
