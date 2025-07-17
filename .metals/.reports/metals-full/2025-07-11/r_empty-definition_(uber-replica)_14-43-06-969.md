error id: file://<WORKSPACE>/src/main/scala/UpsertDriverCurrLocations.scala:
file://<WORKSPACE>/src/main/scala/UpsertDriverCurrLocations.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1450
uri: file://<WORKSPACE>/src/main/scala/UpsertDriverCurrLocations.scala
text:
```scala
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables._
import java.io.File
import  scala.reflect.io.File


import scala.reflect.io.File

object UpsertDriverCurrLocations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UpsertLatestDriverData")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    import spark.implicits._

    val driverSchema = StructType(Seq(
      StructField("driver_id", IntegerType, nullable = false),
      StructField("driver_location", IntegerType, nullable = true),
      StructField("is_available", BooleanType, nullable = true),
      StructField("event_time", TimestampType, nullable = true)
    ))

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("subscribe", "driver_status")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .load()

    val data = kafkaDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), driverSchema).as("data"))
      .select("data.*")

    val deltaTablePath = "data/delta/dri@@ver_current_locations"

    // Check if the path exists and is non-empty
    val path = new java.io.File(deltaTablePath)
    val pathExists = path.exists()
    val pathEmpty = path.listFiles() == null || path.listFiles().isEmpty


    if (!pathExists || pathEmpty) {
      val emptyDF = spark.createDataFrame(Seq.empty[(Int, Int, Boolean, java.sql.Timestamp)])
        .toDF("driver_id", "driver_location", "is_available", "event_time")

      emptyDF.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .save(deltaTablePath)

      println("✅ Created empty Delta table at: " + deltaTablePath)
    }

    val query = data.writeStream
      .foreachBatch { (batchDF: DataFrame, _: Long) =>
        val deltaTable = DeltaTable.forPath(spark, deltaTablePath)

        // Deduplicate: keep only the latest event_time per driver_id
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
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 