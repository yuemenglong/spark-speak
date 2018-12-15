package io.github.yuemenglong.speak

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
    tasks.zip(executors).foreach { case (t, e) => e.execute(t) } // TODO taskPool
  }
}

trait RDD[T] {
  val sc: SpeakContext

  def map[R](fn: T => R): RDD[R] = new Mapped(this, fn)

  def foreach(fn: T => Unit): Unit = {
    val counter = new AtomicLong()
    val tasks = (0 until partition).map(i => new Task {
      override def execute(): Unit = {
        getData(i).foreach(fn)
      }

      override def finish(): Unit = {
        if (counter.incrementAndGet() == partition) {
          counter.synchronized(counter.notify())
        }
      }
    }).toArray
    sc.run(tasks)
    counter.synchronized(counter.wait())
  }

  def partition: Int

  def getData(split: Int): Stream[T]
}

class ParalellizeRDD[T](val sc: SpeakContext, seq: Seq[T], part: Int) extends RDD[T] {
  override def partition: Int = part

  override def getData(split: Int): Stream[T] = {
    val start = seq.length / partition * split
    val end = seq.length / partition * (split + 1)
    (start until end).toStream.map(seq(_))
  }
}

class Mapped[U, T](parent: RDD[U], fn: U => T) extends RDD[T] {
  override def partition: Int = parent.partition

  override def getData(split: Int): Stream[T] = parent.getData(split).map(fn)

  override val sc: SpeakContext = parent.sc
}

trait Task {
  def execute()

  def finish()
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
        task.finish()
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
