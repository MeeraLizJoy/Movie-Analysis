package de.meera.movieanalysis
package RDDAnalysis

import org.apache.spark.sql.SparkSession

object Top10MostViewedMovies {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Top 10 Most Viewed Movies ===")

    val sc = spark.sparkContext

    val moviesRDD = sc.textFile("data/movies.dat")
      .map(_.split("::"))
      .map(arr => (arr(0).toInt, arr(1)))

    val ratingsRDD = sc.textFile("data/ratings.dat")
      .map(_.split("::"))
      .map(arr => (arr(1).toInt, 1))

    val ratingsCount = ratingsRDD.reduceByKey(_ + _)

    val moviesWithRatings = ratingsCount.join(moviesRDD)

    val top10 = moviesWithRatings
      .map{case(movieId, (count, title)) => (count, title)}
      .sortByKey(ascending = false)
      .take(10)

    println("Count\tTitle")
    top10.foreach{case(count, title) => println(s"$count\t$title")}

    val top10DF = spark.createDataFrame(top10).toDF("Count", "Title")
    top10DF.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/RDD/Top10MostViewedMovies")

    println("Top 10 Most Viewed Movies are saved to output/RDD/Top10MostViewedMovies")
  }
}
