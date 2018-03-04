package com.qiuchen.movie

import org.apache.spark._

/**
  * 看过“Lord of the Rings, The (1978)”用户和年龄性别分布
  */
object MovieUserAnalyzer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MovieUserAnalyzer")
    val sc = new SparkContext(conf)
    val DATA_PATH = "data/ml-1m"
    val MOVIE_TITLE = "Lord of the Rings, The (1978)"
    var movie_id_lord: String = null

    //获取Lord of the Rings, The (1978)的movieID
    val movieRDD = sc.textFile(DATA_PATH + "/movies.dat")
    movieRDD.map(x => x.split("::")).filter(x => x(1).equals(MOVIE_TITLE)).take(1).foreach(x => movie_id_lord = x(0))

    //查找所有Lord of the Rings, The (1978)观看记录ratings: RDD[(userID, 1)]
    val ratingsRDD = sc.textFile(DATA_PATH + "/ratings.dat")
    val ratings = ratingsRDD.map(x => x.split("::")).filter(x => x(1).equals(movie_id_lord)).map(x => (x(0), 1))

    //查找用户信息users: RDD[(userID, (gender, age))
    val usersRDD = sc.textFile(DATA_PATH + "/users.dat")
    val users = usersRDD.map(x => x.split("::")).map(x => (x(0), (x(1), x(2))))

    //使用观看过Lord of the Rings, The (1978)所有UserID表关联用户信息表userRatings: RDD[(userID, ((gender, age), 1)]
    val userRatings = users.join(ratings)

    //统计用户分布userDistribution: RDD[((gender, age), cnt)]
    val userDistribution = userRatings.map(i => (i._2._1, i._2._2)).reduceByKey(_ + _)
    //userDistribution.sortBy(x => x._2, false).collect().foreach(println) 按照观看电影数量降序排列
    userDistribution.collect().foreach(println)
    sc.stop()
  }

}
