package com.qiuchen.movie

import org.apache.spark._
/**
  * 看过“Lord of the Rings, The (1978)”用户和年龄性别分布
  */
object MovieUserAnalyzer extends App {
  val conf = new SparkConf().setMaster("local").setAppName("MovieUserAnalyzer")
  val sc = new SparkContext(conf)
  val DATA_PATH = "data/ml-1m"
  val MOVIE_TITLE = "Lord of the Rings, The (1978)"
  var movie_id_lord : String = null

  //获取Lord of the Rings, The (1978)的movieID
  val movieRDD = sc.textFile(DATA_PATH + "/movies.dat")
  movieRDD.map(x => x.split("::")).filter(x => x(1).equals(MOVIE_TITLE)).take(1).foreach(x => movie_id_lord = x(0))
  println(movie_id_lord)

  //查找所有Lord of the Rings, The (1978)观看记录
  val ratingsRDD = sc.textFile(DATA_PATH + "/ratings.dat")
  val userRatings = ratingsRDD.map(x => x.split("::")).filter(x => x(1).equals(movie_id_lord)).map(x => x(0))

  //查找用户信息
  val usersRDD = sc.textFile(DATA_PATH + "/users.dat")
}
