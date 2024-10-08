package de.meera.movieanalysis
package SQLAnalysis

import org.apache.spark.sql.SparkSession

object OldestReleasedMovies {
  def run(spark: SparkSession): Unit = {
    println("=== Oldest Releases Movies ===")

    spark.sql("USE default")

    val oldestMovies = spark.sql(
      """
        SELECT Title, Year
        FROM movies_table
        WHERE Year = (SELECT MIN(Year) FROM movies_table)
        ORDER By Title
    """)

    oldestMovies.show(truncate = false)

    oldestMovies.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/SQL/OldestReleasedMovies")

    println("Oldest Released Movies saved to output/SQL/OldestReleasedMovies")
  }

}
