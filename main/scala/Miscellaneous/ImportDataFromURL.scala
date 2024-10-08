package de.meera.movieanalysis
package Miscellaneous

import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.io.{File, PrintWriter}

object ImportDataFromURL {
  def run(spark: SparkSession): Unit = {
    println("=== Importing Data from URL ===")

    val url = "https://grouplens.org/datasets/movielens/1m/"
    val localPath = "data/downloaded_movies.dat"

    try {
      val data = Source.fromURL(url).mkString
      val pw = new PrintWriter(new File(localPath))
      pw.write(data)
      pw.close()

      println(s"Data downloaded from $url and saved to $localPath")
    } catch {
      case e: Exception =>
        println(s"Failed to download data from $url: ${e.getMessage}")
    }
  }
}
