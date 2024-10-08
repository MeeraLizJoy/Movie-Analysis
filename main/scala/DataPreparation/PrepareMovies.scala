package de.meera.movieanalysis.DataPreparation

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PrepareMovies {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Preparing Movies Data ===")

    val moviesSchema = StructType(Array(
      StructField("MovieID", IntegerType, nullable = false),
      StructField("Title", StringType, nullable = true),
      StructField("Genres", StringType, nullable = true)
    ))

    val moviesDF = spark.read
      .option("delimiter", "::")
      .schema(moviesSchema)
      .csv("data/movies.dat")

    val moviesWithYearDF = moviesDF
      .withColumn("Year", regexp_extract(col("Title"), "\\((\\d{4})\\)", 1).cast(IntegerType))
      .withColumn("CleanedTitle", regexp_replace(col("Title"), "\\s*\\(\\d{4}\\)", ""))
      .drop("Title")
      .withColumnRenamed("CleanedTitle", "Title")

    val moviesFinalDF = moviesWithYearDF
      .withColumn("GenreList", split(col("Genres"), "\\|"))

    val finalMoviesDF = moviesFinalDF.select("MovieID", "Title", "Year", "GenreList")

    finalMoviesDF.write
      .mode("overwrite")
      .parquet("output/movies_prepared.parquet")

    println("Movies data prepared and saved to output/movies_prepared.parquet")
    finalMoviesDF.show(5, truncate = false)
  }
}
