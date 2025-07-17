package com.uberreplica.driver

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.kafka.streams.StreamsBuilder

import java.io.File
import com.uberreplica.customDelta.CustomDeltaUtils._
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Consumed, KTable, Produced}

object UpsertDetails {

  def readKafkaStream(spark: SparkSession, driverSchema: org.apache.spark.sql.types.StructType, kafkaTopic: String): DataFrame = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("subscribe", kafkaTopic) //"driver_current_status"
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .load()

    kafkaDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), driverSchema).as("data"))
      .select("data.*")
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

//  def main(args: Array[String]): Unit = {
//    val builder = new StreamsBuilder();
//    val driverLocationsKTable: KTable[String, String] = builder.table(
//      "driver_status",
//      Consumed.with(Serdes.String(), Serdes.String())
//    )
//    val spark = createDeltaSparkSession()
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
//    import spark.implicits._
//
//    val driverSchema = getDriverSchema()
//    val data = readKafkaStream(spark, driverSchema, "driver_status")
//    val deltaTablePath = "data/delta/driver_current_locations"
//    val colNames = Seq("driver_id", "driver_location", "is_available", "event_time")
//    initializeDeltaTableIfNeeded(spark, deltaTablePath, driverSchema, colNames)
//    upsertDriverLocationsFromStream(spark, data, deltaTablePath)
//  }

  def main(Args: Array[String]):Unit = {
    val builder = new StreamsBuilder()

    val driverStatusTable: KTable[String, String] = builder.table(
      "driver_status",
      Consumed.with(Serdes.String(), Serdes.String())
    )
    driverStatusTable.toStream.to("driver_current_status")(Produced.with(Serdes.String(), Serdes.String()))

  }
}
