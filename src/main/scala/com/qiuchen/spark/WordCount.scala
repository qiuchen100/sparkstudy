package com.qiuchen.spark

import org.apache.spark._
/**
  * Spark WordCount实例
  * @author 邱晨
  * @version 1.0
  */
object WordCount {
  /**
    * 主程序
    * @param args 传入spark输入文件的完整路径
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("请输入文件路径和运行模式！")
      sys.exit()
    }
    val filePath = "hdfs://master:9000/input/" + args(0)
    val mode = args(1)
    val conf = new SparkConf().setMaster(mode).setAppName("WordCount")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(filePath)
    rdd.flatMap(line => line.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)
    sc.stop()
  }
}
