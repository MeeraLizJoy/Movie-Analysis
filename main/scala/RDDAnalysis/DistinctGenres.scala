package de.meera.movieanalysis
package RDDAnalysis

import org.apache.spark.sql.SparkSession

object DistinctGenres {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Distinct List of Genres ===")

    val sc = spark.sparkContext

    val moviesRDD =sc.textFile("data/movies.dat")
      .map(_.split("::"))
      .map(arr => arr(2))

    val genres = moviesRDD
      .flatMap(_.split("\\|"))
      .distinct()
      .collect()

    println("Genres:")
    genres.foreach(genre => println(genre))

    val genresDF = spark.createDataFrame(genres.map(g => Tuple1(g))).toDF("Genre")
    genresDF.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/RDD/DistinctGenres")

    println("Distinct Genres save to output/RDD/DistinctGenres")
  }
}
