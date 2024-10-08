package de.meera.movieanalysis
package SQLAnalysis

import org.apache.spark.sql.SparkSession

object MoviesPerYear {
  def run(spark: SparkSession): Unit = {
    println("=== Number of Movies Released per Year ===")

    spark.sql("USE default")

    val moviesPerYear = spark.sql(
      """
        SELECT Year, COUNT(*) as MovieCount
        FROM movies_table
        GROUP By Year
        ORDER BY Year
    """)

    moviesPerYear.show(100, truncate = false)

    moviesPerYear.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/SQL/MoviesPerYear")

    println("Number of Movies Released Each Year saved to output/SQL/MoviesPerYear")
  }
}
