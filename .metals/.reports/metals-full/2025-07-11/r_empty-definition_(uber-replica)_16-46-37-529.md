error id: file://<WORKSPACE>/src/main/scala/GetDriverLocation.scala:
file://<WORKSPACE>/src/main/scala/GetDriverLocation.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 522
uri: file://<WORKSPACE>/src/main/scala/GetDriverLocation.scala
text:
```scala
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object GetDriverLocation {

  def parseArgs(args: Array[String]): Int = {
    if (args.length != 1) {
      println("❌ Please provide driver_id as a runtime argument.")
      sys.exit(1)
    }
    args(0).toInt
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("GetDriverLocation")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sq@@l.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
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
    val spark = createSparkSession()
    import spark.implicits._
    val df = loadDeltaTable(spark, deltaTablePath)
    val result = filterDriver(df, driverId)
    printResult(result, driverId)
    spark.stop()
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: 