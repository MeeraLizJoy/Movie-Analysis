package de.meera.movieanalysis.DataPreparation

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object PrepareUsers {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Preparing Users Data ===")

    val usersSchema = StructType(Array(
      StructField("UserID", IntegerType, nullable = false),
      StructField("Gender", StringType, nullable = true),
      StructField("Age", IntegerType, nullable = true),
      StructField("Occupation", IntegerType, nullable = true),
      StructField("ZipCode", StringType, nullable = true)
    ))

    val usersDF = spark.read
      .option("delimiter", "::")
      .schema(usersSchema)
      .csv("data/users.dat")

    usersDF.write
      .mode("overwrite")
      .parquet("output/users_prepared.parquet")

    println("Users data prepared and saved to output/users_prepared.parquet")
    usersDF.show(5, truncate = false)
  }
}
