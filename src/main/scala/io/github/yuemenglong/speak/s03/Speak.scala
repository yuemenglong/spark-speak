package io.github.yuemenglong.speak.s03

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{BlockingDeque, ConcurrentLinkedDeque, LinkedBlockingDeque, TimeUnit}

import scala.collection.mutable.ArrayBuffer

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
  @transient val executors: BlockingDeque[ExecutorProxy] = new LinkedBlockingDeque[ExecutorProxy]()
  (0 until conf.executorNum).map(i => new ExecutorProxy("localhost", 6000 + i)).toArray
    .foreach(executors.put)

  // yarn start java -cp xx.jar com.xx.ExecutorService i+6000
  (0 until conf.executorNum).foreach(i => new ExecutorService(i + 6000))
  //  executors.foreach(_.start())

  def parallelize[T](seq: Seq[T], partition: Int): RDD[T] = {
    new ParalellizeRDD[T](this, seq, partition)
  }

  def run(tasks: Array[Task]): Unit = {
    val counter = new AtomicLong()
    val rpc = new Rpc(9999, {
      case ("Finish", e: ExecutorProxy) => {
        executors.put(e)
        if (counter.incrementAndGet() == tasks.length) {
          counter.synchronized(counter.notify())
        }
        ""
      }
    })
    rpc.start()
    tasks.foreach(t => {
      val e = executors.poll(100000, TimeUnit.SECONDS)
      e.execute(new Task {
        override def execute(): Unit = {
          t.execute()
          Rpc.call("localhost", 9999, ("Finish", e))
        }
      })
    })
    counter.synchronized(counter.wait())
    rpc.stop()
  }
}

object RDD {
  implicit def toPair[K, V](rdd: RDD[(K, V)]): PairRDD[K, V] = new PairRDD(rdd)
}

class PairRDD[K, V](rdd: RDD[(K, V)]) extends RDD[(K, V)] {
  override val sc: SpeakContext = rdd.sc

  override def getPartition: Int = rdd.getPartition

  override def getData(split: Int): Stream[(K, V)] = rdd.getData(split)

  def reduceByKey(fn: (V, V) => V): RDD[(K, V)] = {
    // 1. 相同k的聚合在一起
    val res = new ArrayBuffer[Array[(K, Seq[V])]]
    val rpc = new Rpc(8888, {
      case ("Result", item: Array[(K, Seq[V])]) =>
        res += item
        ""
    })
    rpc.start()
    val tasks = (0 until getPartition).map(split => {
      new Task {
        override def execute(): Unit = {
          val res = rdd.getData(split).groupBy(_._1).mapValues(_.map(_._2)).toArray
          Rpc.call("localhost", 8888, ("Result", res))
        }
      }
    }).toArray
    sc.run(tasks)
    rpc.stop()
    val splits: Array[ArrayBuffer[(K, Seq[V])]] = (0 until getPartition).map(_ => {
      new ArrayBuffer[(K, Seq[V])]()
    }).toArray
    res.flatten.foreach { case (k, vs) =>
      val hash = k.hashCode() % getPartition
      splits(hash) += ((k, vs))
    }
    new ReduceRDD(rdd, splits, fn)
  }
}

class ReduceRDD[K, V](rdd: RDD[(K, V)], splits: Array[ArrayBuffer[(K, Seq[V])]],
                      fn: (V, V) => V) extends RDD[(K, V)] {
  override val sc: SpeakContext = rdd.sc

  override def getPartition: Int = rdd.getPartition

  override def getData(split: Int): Stream[(K, V)] = {
    splits(split).groupBy(_._1).mapValues(arr => {
      arr.flatMap(_._2).reduce(fn)
    }).toStream
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
    val size = Math.ceil(seq.length * 1.0 / part).toInt
    val arr = seq.grouped(size).drop(split)
    arr.next().toStream
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

class ExecutorProxy(host: String, port: Int) extends Serializable {
  def execute(task: Task): Unit = {
    Rpc.call(host, port, ("Execute", task))
  }
}

class ExecutorService(port: Int) {
  val executor = new Executor
  executor.start()
  val rpc = new Rpc(port, {
    case ("Execute", t: Task) =>
      executor.execute(t)
      ""
  })
  rpc.start()
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
    sc.parallelize(1 to 10, 2).map(_ % 3)
      .map((_, 1)).reduceByKey(_ + _).foreach(println)
    println("Finish")
    System.exit(0) // TODO stop
  }
}