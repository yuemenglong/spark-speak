package io.github.yuemenglong.speak.s01

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

class SpeakContext(conf: SpeakConf) {
  val executors: Array[Executor] = (0 until conf.executorNum).map(_ => new Executor).toArray
  executors.foreach(_.start())

  def paralellize[T](seq: Seq[T], partition: Int): RDD[T] = {
    new ParalellizeRDD[T](this, seq, partition)
  }

  def run(tasks: Array[Task]): Unit = {
    val counter = new AtomicLong()
    tasks.zip(executors).foreach { case (t, e) =>
      e.execute(new Task {
        override def execute(): Unit = {
          t.execute()
          if (counter.incrementAndGet() == tasks.length) {
            counter.synchronized(counter.notify())
          }
        }
      })
    } // TODO taskPool
    counter.synchronized(counter.wait())
  }
}

trait RDD[T] {
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

trait Task {
  def execute()
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