package de.meera.movieanalysis
package RDDAnalysis

import org.apache.spark.sql.SparkSession

object MoviesStartingWith {
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=== Movies Starting with Each Letter and Number ===")

    val sc = spark.sparkContext

    // Load movies.dat
    val moviesRDD = sc.textFile("data/movies.dat")
      .map(_.split("::"))
      .filter(arr => arr.length >= 2)
      .map(arr => arr(1))
      .map(title => title.trim)
      .filter(title => title.nonEmpty)

    def getFirstChar(title: String): Option[String] = {
      val firstChar = title.charAt(0).toUpper
      firstChar match {
        case c if c.isLetter || c.isDigit => Some(c.toString)
        case _ => None // Exclude titles starting with other characters
      }
    }

    val firstCharRDD = moviesRDD.flatMap(title => getFirstChar(title))
    val validFirstCharRDD = firstCharRDD.filter(char => char.matches("[A-Z0-9]"))

    val firstCharCounts = validFirstCharRDD
      .map(char => (char, 1))
      .reduceByKey(_ + _)
      .sortByKey(ascending = true)

    val results = firstCharCounts.collect()

    println("Starting Character\tCount")
    results.foreach { case (char, count) =>
      println(s"$char\t\t$count")
    }

    val resultsDF = results.toSeq.toDF("StartingCharacter", "Count")

    resultsDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/RDD/MoviesStartingWith")

    println("Movies Starting with Each Character saved to output/RDD/MoviesStartingWith")
  }
}
