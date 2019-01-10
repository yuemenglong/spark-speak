package io.github.yuemenglong.speak.s03

import java.util
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

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

case class Finish(e: ExecutorProxy)

case class ShuffleRes[K, V](res: util.HashMap[Int, util.HashMap[K, util.ArrayList[V]]])

class SpeakContext(conf: SpeakConf) extends Serializable {
  @transient val executors: Array[ExecutorProxy] = (0 until conf.executorNum).map(_ => new ExecutorProxy).toArray
  @transient val scRpc = new Rpc(6666, {
    case Register(id, host, port) =>
      executors(id).host = host
      executors(id).port = port
      println(id, host, port)
      OK()
  })
  scRpc.start()
  // TODO run on yarn
  (0 until conf.executorNum).map(i => new ExecutorService(i, "localhost", 6666, 6667 + i))
  //  executors.foreach(_.start())

  def parallelize[T](seq: Seq[T], partition: Int): RDD[T] = {
    new ParalellizeRDD[T](this, seq, partition)
  }

  def run(tasks: Array[_ <: Task]): Unit = {
    val counter = new AtomicLong(tasks.length)
    val ab = new ArrayBuffer() ++ executors
    val port = Util.testRandPort()
    val rpc = new Rpc(port, {
      case Finish(e) =>
        ab += e
        ab.synchronized(ab.notify())
        if (counter.decrementAndGet() == 0) {
          counter.synchronized(counter.notify())
        }
        OK()
    }).start()
    tasks.foreach { t =>
      val e = if (ab.nonEmpty) {
        ab.remove(0)
      } else {
        ab.synchronized(ab.wait())
        ab.remove(0)
      }
      e.execute(new Task {
        val task: Task = t
        val executor: ExecutorProxy = e

        override def execute(): Unit = {
          task.execute()
          Rpc.call("localhost", port, Finish(executor))
        }
      })
    }
    counter.synchronized(counter.wait())
    rpc.stop()
  }

  def sendMaster(req: Object): Object = {
    Rpc.call("localhost", 6666, req)
  }
}

object RDD {
  implicit def toPair[K, V](rdd: RDD[(K, V)]): PairFn[K, V] = {
    new PairFn(rdd)
  }
}

class PairFn[K, V](rdd: RDD[(K, V)]) {
  def reduceByKey[R](fn: (V, V) => V): RDD[(K, V)] = {
    new ShuffleRDD(rdd).map { case (k, seq) =>
      (k, seq.reduce(fn))
    }
  }
}

trait RDD[T] extends Serializable {
  val sc: SpeakContext

  def map[R](fn: T => R): RDD[R] = new MappedRDD(this, fn)

  def getDeps: Array[RDD[_]]

  def foreach(fn: T => Unit): Unit = {
    def doReady(rdd: RDD[_]): Unit = {
      rdd.getDeps.foreach(doReady)
      if (!rdd.isReady && rdd.isInstanceOf[ShuffleRDD[_, _]]) {
        rdd.asInstanceOf[ShuffleRDD[_, _]].runShuffleTask()
      }
    }

    doReady(this)

    val tasks = (0 until getPartition).map(i => new Task {
      override def execute(): Unit = {
        getData(i).foreach(fn)
      }
    }).toArray
    sc.run(tasks)
  }

  def getPartition: Int

  def getData(split: Int): Stream[T]

  def isReady: Boolean
}

class ShuffleRDD[K, V](dep: RDD[(K, V)]) extends RDD[(K, Seq[V])] {
  val blocks: util.ArrayList[util.HashMap[Int, util.HashMap[K, util.ArrayList[V]]]] =
    new util.ArrayList[util.HashMap[Int, util.HashMap[K, util.ArrayList[V]]]]()
  var ready: Boolean = false
  override val sc: SpeakContext = dep.sc

  override def getPartition: Int = dep.getPartition

  override def getDeps: Array[RDD[_]] = Array(dep)

  override def getData(split: Int): Stream[(K, Seq[V])] = {
    val merged = new util.HashMap[K, util.ArrayList[V]]
    blocks.map(_.get(split)).foreach(map => {
      map.foreach { case (k, arr) =>
        merged.putIfAbsent(k, new util.ArrayList[V]())
        merged.get(k).addAll(arr)
      }
    })
    merged.toMap.mapValues(_.toSeq).toStream
  }

  def runShuffleTask(): Unit = {
    val port = Util.testRandPort()
    val rpc = new Rpc(port, {
      case ShuffleRes(map) =>
        println("AddBlock")
        blocks.add(map.asInstanceOf[util.HashMap[Int, util.HashMap[K, util.ArrayList[V]]]])
        OK()
    }).start()

    val tasks = (0 until getPartition).map(i => new Task {
      val rdd: RDD[(K, V)] = dep
      val split: Int = i

      override def execute(): Unit = {
        val blocks = new util.HashMap[Int, util.HashMap[K, util.ArrayList[V]]]
        rdd.getData(split).foreach { case (k, v) =>
          val idx = k.hashCode() % getPartition
          blocks.putIfAbsent(idx, new util.HashMap[K, util.ArrayList[V]]())
          blocks.get(idx).putIfAbsent(k, new util.ArrayList[V]())
          blocks.get(idx).get(k).add(v)
        }
        Rpc.call("localhost", port, ShuffleRes(blocks))
      }
    }).toArray
    sc.run(tasks)
    rpc.stop()
    ready = true
  }

  override def isReady: Boolean = ready
}

class ParalellizeRDD[T](val sc: SpeakContext, seq: Seq[T], part: Int) extends RDD[T] {
  override def getPartition: Int = part

  def splitSize: Int = Math.ceil(seq.length * 1.0 / getPartition).toInt

  override def getData(split: Int): Stream[T] = {
    val start = splitSize * split
    val end = Math.min(splitSize * (split + 1), seq.length)
    (start until end).toStream.map(seq(_))
  }

  override def getDeps: Array[RDD[_]] = Array()

  override def isReady: Boolean = true
}

class MappedRDD[U, T](dep: RDD[U], fn: U => T) extends RDD[T] {
  override def getPartition: Int = dep.getPartition

  override def getData(split: Int): Stream[T] = dep.getData(split).map(fn)

  override val sc: SpeakContext = dep.sc

  override def getDeps: Array[RDD[_]] = Array(dep)

  override def isReady: Boolean = dep.isReady
}

trait Task extends Serializable {
  def execute()
}

class ExecutorProxy extends Serializable {
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
    sc.parallelize(1 to 20, 3).map(i => (i % 3, i)).reduceByKey(_ + _).foreach(println)
  }
}
