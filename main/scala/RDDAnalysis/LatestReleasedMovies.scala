package de.meera.movieanalysis
package RDDAnalysis

import org.apache.spark.sql.SparkSession

object LatestReleasedMovies {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Latest Released Movies ===")

    val sc = spark.sparkContext

    // Load movies.dat
    val moviesRDD = sc.textFile("data/movies.dat")
      .map(_.split("::"))
      .map(arr => (arr(0).toInt, arr(1)))

    val moviesWithYear = moviesRDD.mapValues{ title =>
      val yearRegex = "\\((\\d{4})\\)".r
      val year = yearRegex.findFirstMatchIn(title).map(_.group(1).toInt).getOrElse(0)
      (title.replaceAll("\\s*\\(\\d{4}\\)", ""), year)
    }

    val latestYear = moviesWithYear.map(_._2._2).max()

    val latestMovies = moviesWithYear.filter{case(_, (_, year)) => year == latestYear }
      .map{case(_, (title, year)) => (title, year)}
      .collect()

    println(s"Latest Released Movies (Year: $latestYear):")
    println("Title\tYear")
    latestMovies.foreach { case (title, year) => println(s"$title\t$year") }

    val latestMoviesDF = spark.createDataFrame(latestMovies).toDF("Title", "Year")
    latestMoviesDF.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/RDD/LatestReleasedMovies")

    println("Latest Released Movies saved to output/RDD/LatestReleasedMovies")

  }
}
