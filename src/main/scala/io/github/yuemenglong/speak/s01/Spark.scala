package io.github.yuemenglong.speak.s01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by <yuemenglong@126.com> on 2018/12/15.
  */
object Spark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Speak").setMaster("local[2]")
    var sc = new SparkContext(conf)
    sc.parallelize(1 to 10, 2).map(_ * 2).foreach(println)
  }
}
