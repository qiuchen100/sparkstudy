package com.qiuchen.movie

import org.apache.spark._

/**
  * 得分最高的10部电影；看过电影最多的前10个人；女性看多最多的10部电影；男性看过最多的10部电影
  */
object TopKMovieAnalyzer extends App {
  val conf = new SparkConf().setMaster("local").setAppName("PopularMovieAnalyzer")
  val sc = new SparkContext(conf)
  val DATA_PATH = "data/ml-1m"

  /**
    * Step 1: Create RDDs
    */
  val usersRDD = sc.textFile(DATA_PATH + "/users.dat")
  val moviesRDD = sc.textFile(DATA_PATH + "/movies.dat")
  val ratingsRDD = sc.textFile(DATA_PATH + "/ratings.dat")

  /**
    * Step 2: 缓存数据ratings: RDD[(userID, movieID, rating)]
    */
  val ratings = ratingsRDD.map(x => x.split("::")).map(x => (x(0), x(1), x(2))).cache()

  /**
    * Step 3: 得分最高的10部电影
    */
  val topKMovieScore = ratings.map(x => (x._2, (x._3.toInt, 1)))
                              .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
                              .map(x => (x._1, x._2._1.toFloat / x._2._2.toFloat))
                              .sortBy(x => x._2, false)
                              .take(10)
                              .foreach(println)
}
