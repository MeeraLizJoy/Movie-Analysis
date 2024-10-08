package de.meera.movieanalysis
package SQLAnalysis

import org.apache.spark.sql.SparkSession

object TotalRatingPerMovie {
  def run(spark: SparkSession): Unit = {
    println("=== Total Rating for Each Movie ===")

    spark.sql("USE default")

    val totalRatingPerMovie = spark.sql("""
      SELECT MovieID, SUM(Rating) as TotalRating
      FROM ratings_table
      GROUP BY MovieID
      ORDER BY TotalRating DESC
    """)

    totalRatingPerMovie.show(10, truncate = false)

    totalRatingPerMovie.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/SQL/TotalRatingPerMovie")

    println("Total Rating for Each Movie saved to output/SQL/TotalRatingPerMovie")
  }
}
