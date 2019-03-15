package io.github.yuemenglong.speak.stemp

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong

/**
  * Created by <yuemenglong@126.com> on 2018/12/15.
  */

class SpeakConf() {
  var executorNum = 0

  def setAppName(name: String): SpeakConf = {
    this
  }

  def setMaster(master: String): SpeakConf = {
    executorNum = master.split("\\[")(1).split("\\]")(0).toInt
    this
  }
}

class SpeakContext(conf: SpeakConf) extends Serializable {
  @transient val executors: Array[ExecutorProxy] = (0 until conf.executorNum).map(i => new ExecutorProxy("localhost", 6000 + i)).toArray
  // java -cp xx.jar com.xx.ExecutorService i+6000
  (0 until conf.executorNum).foreach(i => new ExecutorService(i + 6000))
  // wait executor setup
  //  executors.foreach(_.start())

  def paralellize[T](seq: Seq[T], partition: Int): RDD[T] = {
    new ParalellizeRDD[T](this, seq, partition)
  }

  def run(tasks: Array[Task]): Unit = {
    val counter = new AtomicLong()
    val rpc = new Rpc(9999, {
      case "Finish" =>
        if (counter.incrementAndGet() == tasks.length) {
          counter.synchronized(counter.notify())
        }
        "OK"
    })
    rpc.start()
    tasks.zip(executors).foreach { case (t, e) =>
      e.execute(new Task {
        override def execute(): Unit = {
          t.execute()
          Rpc.call("localhost", 9999, "Finish")
          //          if (counter.incrementAndGet() == tasks.length) {
          //            counter.synchronized(counter.notify())
          //          }
        }
      })
    } // TODO taskPool
    counter.synchronized(counter.wait())
    rpc.stop()
  }
}

trait RDD[T] extends Serializable {
  val sc: SpeakContext

  def map[R](fn: T => R): RDD[R] = new Mapped(this, fn)

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

  override def getData(split: Int): Stream[T] = {
    val start = seq.length / getPartition * split
    val end = seq.length / getPartition * (split + 1)
    (start until end).toStream.map(seq(_))
  }
}

class Mapped[U, T](parent: RDD[U], fn: U => T) extends RDD[T] {
  override def getPartition: Int = parent.getPartition

  override def getData(split: Int): Stream[T] = parent.getData(split).map(fn)

  override val sc: SpeakContext = parent.sc
}

trait Task extends Serializable {
  def execute()
}

class ExecutorProxy(host: String, port: Int) {

  def execute(task: Task): Unit = {
    Rpc.call(host, port, ("Execute", task))
  }
}

class ExecutorService(port: Int) {
  val rpc = new Rpc(port, {
    case ("Execute", t) =>
      val task = t.asInstanceOf[Task]
      executor.execute(task)
      "OK"
  })
  rpc.start()
  val executor = new Executor
  executor.start()
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