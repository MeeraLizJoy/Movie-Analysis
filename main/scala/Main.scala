package de.meera.movieanalysis

import DataPreparation._
import Miscellaneous._
import RDDAnalysis._
import SQLAnalysis._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Movie Analysis")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .enableHiveSupport()
      .getOrCreate()

    try {
      println("=== Data Preparation ===")
      PrepareMovies.run(spark)
      PrepareUsers.run(spark)
      PrepareRatings.run(spark)

      println("\n=== RDD Analysis ===")
      Top10MostViewedMovies.run(spark)
      DistinctGenres.run(spark)
      MoviesPerGenre.run(spark)
      MoviesStartingWith.run(spark)
      LatestReleasedMovies.run(spark)

      println("\n=== SQL Analysis ===")
      CreateTables.run(spark)
      OldestReleasedMovies.run(spark)
      MoviesPerYear.run(spark)
      MoviesPerRating.run(spark)
      UsersPerMovie.run(spark)
      TotalRatingPerMovie.run(spark)
      AverageRatingPerMovie.run(spark)

      println("\n=== Miscellaneous Tasks ===")
      ImportDataFromURL.run(spark)
      SaveTableWithoutDDL.run(spark)
      BroadcastVariableExample.run(spark)
      AccumulatorExample.run(spark)

      println("=== All Tasks Completed Successfully ===")
    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}