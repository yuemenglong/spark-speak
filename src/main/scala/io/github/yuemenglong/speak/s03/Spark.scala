package io.github.yuemenglong.speak.s03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by <yuemenglong@126.com> on 2018/12/15.
  */
object Spark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Speak").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.parallelize(1 to 10, 2).map(_ % 3)
      .map((_, 1)).reduceByKey(_ + _).foreach(println)
  }
}
