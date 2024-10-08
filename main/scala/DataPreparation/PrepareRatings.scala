package de.meera.movieanalysis.DataPreparation

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object PrepareRatings {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Preparing Ratings Data ===")

    val ratingsSchema = StructType(Array(
      StructField("UserID", IntegerType, nullable = false),
      StructField("MovieID", IntegerType, nullable = false),
      StructField("Rating", IntegerType, nullable = false),
      StructField("Timestamp", LongType, nullable = true)
    ))

    val ratingsDF = spark.read
      .option("delimiter", "::")
      .schema(ratingsSchema)
      .csv("data/ratings.dat")

    val ratingsWithDataDF = ratingsDF
      .withColumn("Date", from_unixtime(col("Timestamp")).cast("timestamp"))
      .drop("Timestamp")

    ratingsWithDataDF.write
      .mode("overwrite")
      .parquet("output/ratings_prepared.parquet")

    println("Ratings data prepared and saved to output/ratings_prepared.parquet")
    ratingsWithDataDF.show(5, truncate = false)
  }
}
