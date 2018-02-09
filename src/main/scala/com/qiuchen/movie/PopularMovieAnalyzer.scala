package com.qiuchen.movie

import org.apache.spark._

/**
  * 年龄段在“18-24”的男性年轻人，最喜欢看哪10部电影
  */
object PopularMovieAnalyzer extends App {

  val conf = new SparkConf().setMaster("local").setAppName("MovieUserAnalyzer")
  val sc = new SparkContext(conf)
  val DATA_PATH = "data/ml-1m"
  val usersRDD = sc.textFile(DATA_PATH + "/users.dat")
  val moviesRDD = sc.textFile(DATA_PATH + "/movies.dat")
  val ratingsRDD = sc.textFile(DATA_PATH + "/ratings.dat")

  //查找年龄段18-24的所有用户ID users: RDD[(userID, age)]
  val users = usersRDD.map(x => x.split("::")).map(x => (x(0), x(2).toInt)).filter(x => x._2 >= 18 && x._2 <= 24)
  //查找电影观看记录信息ratings: RDD[(userID, MovieID)]
  val ratings = ratingsRDD.map(x => x.split("::")).map(x => (x(0), x(1)))
  //查找电影信息movies: RDD[(MovieID, Title)]
  val movies = moviesRDD.map(x => x.split("::")).map(x => (x(0), x(1)))
  //查找年龄段18-24的用户电影观看记录 userRatings: RDD[(MovieID, 1)]
  val userRatings = ratings.join(users).map(x => (x._2._1, 1))

  //统计电影观看记录次数，并取得前10名的名额
  val moviesCount = userRatings.join(movies).map(x => (x._2._2, x._2._1)).reduceByKey(_ + _).sortBy(x => x._2, false).take(10)
  moviesCount.foreach(println)

}
