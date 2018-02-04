package com.qiuchen.spark

/* 作者：邱晨
 * 功能：实现PI运算
 */

import org.apache.spark._
import scala.math.random

object Pi extends App {
  val conf = new SparkConf().setAppName("PI Cal").setMaster("local")
  val sc = new SparkContext(conf)
  val slices = 10
  val n = 1000000 * slices
  val count = sc.parallelize(1 to n, slices).map{ i =>
    val x = random * 2 - 1
    val y = random * 2 - 1
    if(x * x + y * y < 1) 1 else 0
  }.reduce(_ + _)
  println("Pi is roughly " + 4.0 * count / (n - 1))
}
