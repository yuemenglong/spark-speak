package io.github.yuemenglong.speak.s03

import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}

/**
  * Created by <yuemenglong@126.com> on 2018/12/15.
  */
object Spark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Speak").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.parallelize(1 to 20, 3).map(i => (i % 3, i)).groupByKey().map { case (k, seq) =>
      (k, seq.reduce(_ + _))
    }.foreach(println)
  }
}

class A extends org.apache.spark.rdd.RDD[String](null, null) {
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    new Iterator[String] {
      override def hasNext: Boolean = ???

      override def next(): String = ???
    }
  }

  override protected def getPartitions: Array[Partition] = ???
}
