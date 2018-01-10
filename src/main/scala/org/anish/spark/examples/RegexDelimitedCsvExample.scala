package org.anish.spark.examples

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by anish on 10/01/2018.
  */
object RegexDelimitedCsvExample {
  val filePath: String = "data/rsv/"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[8]").appName("SkewJoinDataCreator").getOrCreate()

    val tmp = "tmp/spark-regex-csv/output"

    val df = sparkSession
      .read
      .format("org.anish.spark.regexcsv")
      .option("header", "true")
      .option("delimiter", ",|;")
      .load(filePath)

    df.printSchema()

    df.show()

    println("Trying to save the df: ")

     df
       .write
       .format("org.anish.spark.regexcsv")
       .option("header", "true")
       .option("delimiter", ",") // You cannot save data using regex.
       .mode(SaveMode.Overwrite)
       .save(tmp)

    println(s"df saved successfully to $tmp")
  }
}
