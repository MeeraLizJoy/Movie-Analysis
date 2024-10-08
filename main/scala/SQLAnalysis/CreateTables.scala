package de.meera.movieanalysis
package SQLAnalysis

import org.apache.spark.sql.SparkSession

object CreateTables {
  def run(spark: SparkSession): Unit = {
    println("=== Creating Hive Tables ===")

    val moviesDF = spark.read.parquet("output/movies_prepared.parquet")
    val usersDF = spark.read.parquet("output/users_prepared.parquet")
    val ratingsDF = spark.read.parquet("output/ratings_prepared.parquet")

    moviesDF.write.mode("overwrite").saveAsTable("movies_table")
    usersDF.write.mode("overwrite").saveAsTable("users_table")
    ratingsDF.write.mode("overwrite").saveAsTable("ratings_table")

    println("Hive tables 'movies_table', 'users_table', and 'ratings_table' created successfully.")
  }
}
