package io.github.yuemenglong.speak.s02

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong

/**
  * Created by <yuemenglong@126.com> on 2018/12/15.
  */

class SpeakConf extends Serializable {
  var executorNum = 0

  def setAppName(name: String): SpeakConf = {
    this
  }

  def setMaster(master: String): SpeakConf = {
    executorNum = master.split("\\[")(1).split("\\]")(0).toInt
    this
  }
}

case class OK()

case class Register(id: Int, host: String, port: Int)

case class Execute(task: Task)

case class Finish()

class SpeakContext(conf: SpeakConf) extends Serializable {
  @transient val executors: Array[ExecutorProxy] = (0 until conf.executorNum).map(_ => new ExecutorProxy).toArray
  @transient val rpc = new Rpc(6666, {
    case Register(id, host, port) =>
      executors(id).host = host
      executors(id).port = port
      println(id, host, port)
      OK()
  })
  rpc.start()
  // TODO run on yarn
  (0 until conf.executorNum).map(i => new ExecutorService(i, "localhost", 6666, 6667 + i))
  //  executors.foreach(_.start())

  def paralellize[T](seq: Seq[T], partition: Int): RDD[T] = {
    new ParalellizeRDD[T](this, seq, partition)
  }

  def run(tasks: Array[Task]): Unit = {
    val counter = new AtomicLong(tasks.length)
    val jobRpc = new Rpc(9999, {
      case Finish() =>
        if (counter.decrementAndGet() == 0) {
          counter.synchronized(counter.notify())
        }
        OK()
    }).start()
    tasks.zip(executors).foreach { case (t, e) =>
      e.execute(new Task {
        val task: Task = t

        override def execute(): Unit = {
          task.execute()
          Rpc.call("localhost", 9999, Finish())
        }
      })
    } // TODO taskPool
    counter.synchronized(counter.wait())
    jobRpc.stop()
  }

  def sendMaster(req: Object): Object = {
    Rpc.call("localhost", 6666, req)
  }
}

trait RDD[T] extends Serializable {
  val sc: SpeakContext

  def map[R](fn: T => R): RDD[R] = new MappedRDD(this, fn)

  def foreach(fn: T => Unit): Unit = {
    val tasks = (0 until getPartition).map(i => new Task {
      override def execute(): Unit = {
        getData(i).foreach(fn)
      }
    }).toArray
    sc.run(tasks)
  }

  def getPartition: Int

  def getData(split: Int): Stream[T]
}

class ParalellizeRDD[T](val sc: SpeakContext, seq: Seq[T], part: Int) extends RDD[T] {
  override def getPartition: Int = part

  def splitSize: Int = Math.ceil(seq.length * 1.0 / getPartition).toInt

  override def getData(split: Int): Stream[T] = {
    val start = splitSize * split
    val end = Math.min(splitSize * (split + 1), seq.length)
    (start until end).toStream.map(seq(_))
  }
}

class MappedRDD[U, T](parent: RDD[U], fn: U => T) extends RDD[T] {
  override def getPartition: Int = parent.getPartition

  override def getData(split: Int): Stream[T] = parent.getData(split).map(fn)

  override val sc: SpeakContext = parent.sc
}

trait Task extends Serializable {
  def execute()
}

class ExecutorProxy {
  var host = ""
  var port = 0

  def send(req: Object): Object = {
    Rpc.call(host, port, req)
  }

  def execute(task: Task): Unit = {
    send(Execute(task))
  }
}

class ExecutorService(id: Int, mhost: String, mport: Int, port: Int) {
  val executor = new Executor
  executor.start()
  val rpc = new Rpc(port, {
    case Execute(task) =>
      executor.execute(task)
      OK
  })
  rpc.start()
  sendMaster(Register(id, "localhost", port))

  def sendMaster(req: Object): Object = {
    Rpc.call(mhost, mport, req)
  }
}

class Executor extends Thread {
  val tasks = new ConcurrentLinkedDeque[Task]()

  def execute(task: Task): Unit = {
    tasks.addLast(task)
  }

  override def run(): Unit = {
    while (true) {
      val task = tasks.pollFirst()
      if (task == null) {
        Thread.sleep(1)
      } else {
        task.execute()
      }
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SpeakConf().setMaster("local[2]").setAppName("Speak")
    val sc = new SpeakContext(conf)
    sc.paralellize(1 to 10, 2).map(_ * 2).foreach(println)
    println("Finish")
    System.exit(0) // TODO stop
  }
}
