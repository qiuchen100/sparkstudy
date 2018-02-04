package com.qiuchen.spark

import org.apache.spark._

/**
  * Spark运行测试
  *
  */
object StartSpark extends App {
  val conf = new SparkConf().setMaster("local").setAppName("Start Spark")
  val sc = new SparkContext(conf)
  val rdd = sc.parallelize(List(1, 2, 3, 4, 5), 3)
  rdd.map(_ + 3).filter(_ > 4).foreach(print)
}
