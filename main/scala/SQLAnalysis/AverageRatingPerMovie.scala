package de.meera.movieanalysis
package SQLAnalysis

import org.apache.spark.sql.SparkSession

object AverageRatingPerMovie {
  def run(spark: SparkSession): Unit = {
    println("=== Average Rating for Each Movie ===")

    spark.sql("USE default")

    val averageRatingPerMovie = spark.sql("""
      SELECT MovieID, AVG(Rating) as AverageRating
      FROM ratings_table
      GROUP BY MovieID
      ORDER BY AverageRating DESC
    """)

    averageRatingPerMovie.show(10, truncate = false)

    averageRatingPerMovie.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/SQL/AverageRatingPerMovie")

    println("Average Rating for Each Movie saved to output/SQL/AverageRatingPerMovie")
  }
}
