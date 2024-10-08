package de.meera.movieanalysis
package Miscellaneous

import org.apache.spark.sql.SparkSession

object BroadcastVariableExample {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Broadcast Variable Example ===")

    val sc = spark.sparkContext

    val genresList = List("Action", "Comedy", "Drama", "Horror", "Sci-Fi")
    val broadcastGenres = sc.broadcast(genresList)

    val moviesRDD = sc.textFile("data/movies.dat")
      .map(_.split("::"))
      .map(arr => arr(2))

    val filteredMovies = moviesRDD.filter { genres =>
      genres.split("\\|").exists(g => broadcastGenres.value.contains(g))
    }.collect()

    println("Movies containing broadcasted genres:")
    filteredMovies.foreach(println)

    val filteredMoviesDF = filteredMovies.toSeq.toDF("Genres")
    filteredMoviesDF.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/Miscellaneous/BroadcastVariableExample")

    println("Filtered Movies with Broadcast Variable saved to output/Miscellaneous/BroadcastVariableExample")
  }
}
