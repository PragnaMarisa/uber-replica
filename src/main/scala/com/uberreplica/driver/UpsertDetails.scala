package com.uberreplica.driver

import com.uberreplica.customDelta.CustomDeltaUtils._
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object UpsertDetails {

  def readKafkaStream(spark: SparkSession, driverSchema: org.apache.spark.sql.types.StructType, kafkaTopic: String): DataFrame = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("subscribe", kafkaTopic)
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .load()

    kafkaDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), driverSchema).as("data"))
      .select("data.*")
  }

  def createSparkSessionToReadDeltaFiles(): SparkSession = {
    SparkSession.builder()
      .appName("GetDriverLocation")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  def upsertDriverLocationsFromStream(spark: SparkSession, data: DataFrame, deltaTablePath: String): Unit = {
    val query = data.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        val deltaTable = DeltaTable.forPath(spark, deltaTablePath)

        val latestBatchDF = batchDF
          .orderBy(desc("event_time"))
          .dropDuplicates("driver_id")

        deltaTable
          .as("target")
          .merge(
            latestBatchDF.as("source"),
            "target.driver_id = source.driver_id"
          )
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
      }
      .option("checkpointLocation", "data/checkpoints/driver_status")
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val spark = createDeltaSparkSession()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    import spark.implicits._

    val driverSchema = getDriverSchema()
    val data = readKafkaStream(spark, driverSchema, "driver_status")
    val deltaTablePath = "data/delta/driver_current_locations"
    val colNames = Seq("driver_id", "driver_location", "is_available", "event_time")
    initializeDeltaTableIfNeeded(spark, deltaTablePath, driverSchema, colNames)
    upsertDriverLocationsFromStream(spark, data, deltaTablePath)
  }

}
