package de.meera.movieanalysis.Miscellaneous

import org.apache.spark.sql.SparkSession

case class Metric(Metric: String, Count: Long)

object AccumulatorExample {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Accumulator Example ===")

    val sc = spark.sparkContext

    val badRecords = sc.longAccumulator("BadRecords")

    val moviesRDD = sc.textFile("data/movies.dat")
      .map(line => {
        val fields = line.split("::")
        if (fields.length != 3) {
          badRecords.add(1)
          None
        } else {
          Some((fields(0).toInt, fields(1), fields(2)))
        }
      })
      .filter(_.isDefined)
      .map(_.get)

    val count = moviesRDD.count()

    println(s"Total valid records: $count")
    println(s"Total bad records: ${badRecords.value}")

    val accumulatorData: Seq[Metric] = Seq(
      Metric("Total Valid Records", count),
      Metric("Total Bad Records", badRecords.value)
    )

    val accumulatorDF = accumulatorData.toDF()

    accumulatorDF.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/Miscellaneous/AccumulatorExample")

    println("Accumulator results saved to output/Miscellaneous/AccumulatorExample")
  }
}
