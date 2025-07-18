package com.uberreplica.ride

import com.uberreplica.driver.GetDetails.{loadDeltaTable, printResult}
import com.uberreplica.driver.UpsertDetails.createSparkSessionToReadDeltaFiles
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RideDetails {
  def parseArgs(args: Array[String]): Int = {
    if (args.length != 1) {
      println("❌ Please provide ride_id as a runtime argument.")
      sys.exit(1)
    }
    args(0).toInt
  }


  def filterRide(df: DataFrame, rideId: Int): DataFrame = {
    df.filter(df("ride_id") === rideId)
  }


  def main(args: Array[String]): Unit = {
    val ride_id = parseArgs(args)
    val deltaTablePath = "data/delta/ride_requests"
    val spark = createSparkSessionToReadDeltaFiles()
    import spark.implicits._
    val df = loadDeltaTable(spark, deltaTablePath)
    val result = filterRide(df, ride_id)
    printResult(result, ride_id)
    //    spark.stop()
  }
}
