package com.qiuchen.spark

import org.apache.spark._

object JoinTest extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Join Test")
  val sc = new SparkContext(sparkConf)
  val visits = sc.parallelize(List(
    ("index.html", "1.2.3.4"),
    ("about.html", "3.4.5.6"),
    ("index.html", "1.3.3.1")))
  val pageNames = sc.parallelize(List(
    ("index.html", "home"),
    ("about.html", "about")))

  val resultRdd1 = visits.join(pageNames)
  val resultRdd2 = visits.cogroup(pageNames)
  println("-------------join------------------")
  resultRdd1.foreach(println)
  println("-------------cogroup----------------")
  resultRdd2.foreach(println)
}
