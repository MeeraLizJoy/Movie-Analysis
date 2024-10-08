package de.meera.movieanalysis
package RDDAnalysis

import org.apache.spark.sql.SparkSession

object MoviesPerGenre {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Number of Movies per Genre ===")

    val sc = spark.sparkContext

    val moviesRDD = sc.textFile("data/movies.dat")
      .map(_.split("::"))
      .map(arr => arr(2))

    val genreCounts = moviesRDD
      .flatMap(_.split("\\|"))
      .map(genre => (genre, 1))
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)

    println("Genre\tCount")
    genreCounts.foreach{case(genre, count) => println(s"$genre\t$count")}

    val genreCountsDF = spark.createDataFrame(genreCounts).toDF("Genre", "Count")
    genreCountsDF.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/RDD/MoviesPerGenre")

    println("Number of Movies per Genre saved to output/RDD/MoviesPerGenre")
  }
}
