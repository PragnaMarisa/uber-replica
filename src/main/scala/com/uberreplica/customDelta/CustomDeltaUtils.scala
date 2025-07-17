package com.uberreplica.customDelta

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.io.File

object CustomDeltaUtils {
  def createEmptyDF(spark: SparkSession, schema: StructType, colNames: Seq[String]): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema).toDF(colNames: _*)
  }

  def createDeltaSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("UberReplicaApp")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  def getDriverSchema(): StructType = {
    StructType(Seq(
      StructField("driver_id", IntegerType, nullable = false),
      StructField("driver_location", IntegerType, nullable = true),
      StructField("is_available", BooleanType, nullable = true),
      StructField("event_time", TimestampType, nullable = true)
    ))
  }

  def getRideRequestSchema(): StructType = {
    StructType(Seq(
      StructField("ride_id", IntegerType, nullable = false),
      StructField("passenger_id", IntegerType, nullable = false),
      StructField("pickup_location", LongType, nullable = false),
      StructField("ride_status", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("ride_date", DateType, nullable = true)
    ))
  }

  def initializeDeltaTableIfNeeded(spark: SparkSession, deltaTablePath: String, schema: StructType, colNames: Seq[String]): Unit = {
    val path = new File(deltaTablePath)
    val pathExists = path.exists()
    val pathEmpty = path.listFiles() == null || path.listFiles().isEmpty

    if (!pathExists || pathEmpty) {
      val emptyDF = createEmptyDF(spark, schema, colNames)
      emptyDF.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .save(deltaTablePath)
      println("✅ Created empty Delta table at: " + deltaTablePath)
    }
  }
} 