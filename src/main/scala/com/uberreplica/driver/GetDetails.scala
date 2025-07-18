package com.uberreplica.driver

import com.uberreplica.driver.UpsertDetails.createSparkSessionToReadDeltaFiles
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object GetDetails {
  def parseArgs(args: Array[String]): Int = {
    if (args.length != 1) {
      println("❌ Please provide driver_id as a runtime argument.")
      sys.exit(1)
    }
    args(0).toInt
  }

  def readKafkaStream(spark: SparkSession, driverSchema: org.apache.spark.sql.types.StructType, kafkaTopic: String): DataFrame = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("subscribe", kafkaTopic)//"driver_current_status"
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .load()

    kafkaDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), driverSchema).as("data"))
      .select("data.*")
  }

  def loadDeltaTable(spark: SparkSession, path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  def filterDriver(df: DataFrame, driverId: Int): DataFrame = {
    df.filter(df("driver_id") === driverId)
  }

  def printResult(result: DataFrame, driverId: Int): Unit = {
    if (result.isEmpty) {
      println(s"🔍 No current location found for driver_id: $driverId")
    } else {
      println(s"✅ Current location of driver_id $driverId:")
      result.show(false)
    }
  }

  def main(args: Array[String]): Unit = {
    val driverId = parseArgs(args)
    val deltaTablePath = "data/delta/driver_current_locations"
    val spark = createSparkSessionToReadDeltaFiles()
    import spark.implicits._
    val df = loadDeltaTable(spark, deltaTablePath)
    val result = filterDriver(df, driverId)
    printResult(result, driverId)
//    spark.stop()
  }
}
