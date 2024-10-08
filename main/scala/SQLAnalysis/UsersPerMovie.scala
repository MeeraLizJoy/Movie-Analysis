package de.meera.movieanalysis
package SQLAnalysis

import org.apache.spark.sql.SparkSession

object UsersPerMovie {
  def run(spark: SparkSession): Unit = {
    println("=== Number of Users Who Rated Each Movie ===")

    spark.sql("USE default")

    val usersPerMovie = spark.sql("""
      SELECT MovieID, COUNT(DISTINCT UserID) as UserCount
      FROM ratings_table
      GROUP BY MovieID
      ORDER BY UserCount DESC
    """)

    usersPerMovie.show(10, truncate = false)

    usersPerMovie.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/SQL/UsersPerMovie")

    println("Number of Users Who Rated Each Movie saved to output/SQL/UsersPerMovie")
  }
}
