error id: file://<WORKSPACE>/src/main/scala/UpsertDriverCurrLocations.scala:local4
file://<WORKSPACE>/src/main/scala/UpsertDriverCurrLocations.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol local4
empty definition using fallback
non-local guesses:

offset: 2370
uri: file://<WORKSPACE>/src/main/scala/UpsertDriverCurrLocations.scala
text:
```scala
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables._
import java.io.File
import org.apache.spark.sql.Row

object UpsertDriverCurrLocations {

  def createEmptyDF(spark: SparkSession, schema: StructType, colNames: Seq[String]): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema).toDF(colNames: _*)
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("UpsertLatestDriverData")
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
      StructField("pickup_location", IntegerType, nullable = false),
      StructField("is_completed", BooleanType, nullable = true),
      StructField("event_time", TimestampType, nullable = true),
      StructField("ride_date", DateType, nullable = true)
    ))
  }

  def readKafkaStream(spark: SparkSession, driverSchema: StructType): DataFrame = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("subscribe", "driver_status")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .load()

    kafkaDF.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), driverSchema).as("data"))
      .select("data.*")
  }

  def initializeDeltaTableIfNeeded(spark: SparkSession, deltaTablePath: String, driverSchema: StructType, colNames): Unit = {
    val path = new java.io.File(deltaTablePath)
    val pathExists = path.exists()
    val pathEmpty = path.listFiles() == null || path.listFiles().isEmpty

    if (!pathExists || pathEmpty) {
      val colN@@ames = Seq("driver_id", "driver_location", "is_available", "event_time")
      val emptyDF = createEmptyDF(spark, driverSchema, colNames)

      emptyDF.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .save(deltaTablePath)

      println("✅ Created empty Delta table at: " + deltaTablePath)
    }
  }

  def startDriverLocationsStreamingUpsert(spark: SparkSession, data: DataFrame, deltaTablePath: String): Unit = {
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
    val spark = createSparkSession()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    import spark.implicits._

    val driverSchema = getDriverSchema()
    val data = readKafkaStream(spark, driverSchema)
    val deltaTablePath = "data/delta/driver_current_locations"
    initializeDeltaTableIfNeeded(spark, deltaTablePath, driverSchema)
    startDriverLocationsStreamingUpsert(spark, data, deltaTablePath)
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 