package de.meera.movieanalysis
package Miscellaneous

import org.apache.spark.sql.SparkSession

object SaveTableWithoutDDL {
  def run(spark: SparkSession): Unit = {
    println("=== Saving Table Without Defining DDL in Hive ===")

    val moviesDF = spark.read.parquet("output/movies_prepared.parquet")

    moviesDF.write.mode("overwrite").saveAsTable("movies_table_no_ddl")

    println("Table 'movies_table_no_ddl' saved successfully without defining DDL.")
  }
}
