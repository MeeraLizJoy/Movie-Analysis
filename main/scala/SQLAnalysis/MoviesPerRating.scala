package de.meera.movieanalysis
package SQLAnalysis

import org.apache.spark.sql.SparkSession

object MoviesPerRating {
  def run(spark: SparkSession): Unit = {
    println("=== Number of Movies for Each Rating ===")

    spark.sql("USE default")

    val moviesPerRating = spark.sql("""
      SELECT Rating, COUNT(DISTINCT MovieID) as MovieCount
      FROM ratings_table
      GROUP BY Rating
      ORDER BY Rating
    """)

    moviesPerRating.show(truncate = false)

    moviesPerRating.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/SQL/MoviesPerRating")

    println("Number of Movies for Each Rating saved to output/SQL/MoviesPerRating")
  }
}
