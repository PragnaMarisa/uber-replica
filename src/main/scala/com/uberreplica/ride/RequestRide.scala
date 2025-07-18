package com.uberreplica.ride

import com.uberreplica.customDelta.CustomDeltaUtils._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Timestamp
import java.time.LocalDate

object RequestRide {
  
  def parseArgs(args: Array[String]): (Int, Long) = {
    if (args.length < 2) {
      println("Passenger Id and PickupLocId is not given");
    }
    val passengerId: Int = args(0).toInt;
    val pickupLocId: Long = args(1).toLong;
    (passengerId, pickupLocId)
  }

  def main(args: Array[String]): Unit = {
    println("Hello")
    val (passengerId, pickupLocId) = parseArgs(args);
    val spark = createDeltaSparkSession()
    val riderequestId: Int = insertRideRequests(spark, passengerId, pickupLocId)
    val driverId: Int = getNearestDriver(pickupLocId)
    println("Driver Id: " + driverId)
    updateRideStatus(spark, "data/delta/ride_requests", riderequestId, "ASSIGNED")
    assignDriverToRide(
      spark,
      "data/delta/ride_requests",
      riderequestId,
      driverId
    )

    updateDriverStatus(spark,"data/delta/driver_current_locations",driverId,false)
    //
  }

  private def getNearestDriver(pickupLocId: Long): Int = {
    val spark = createDeltaSparkSession()
    val driverTablePath = "data/delta/driver_current_locations"
    val df = spark.read.format("delta").load(driverTablePath)
    val availableDrivers = df.filter(df("is_available") === true)
    if (availableDrivers.isEmpty) {
      println("No available drivers found.")
      return -1
    }
    val nearestDriverRow = availableDrivers
      .withColumn("distance", abs(col("driver_location") - lit(pickupLocId)))
      .orderBy(col("distance").asc)
      .limit(1)
      .select("driver_id")
      .collect()
    if (nearestDriverRow.isEmpty) {
      println("No available drivers found after filtering.")
      -1
    } else {
      val driverId = nearestDriverRow(0).getInt(0)
      println(s"Nearest available driver to pickup location $pickupLocId is driver_id: $driverId")
      driverId
    }
  }

  private def getNextRideId(spark: SparkSession, deltaTablePath: String): Int = {
    import spark.implicits._
    val df = spark.read.format("delta").load(deltaTablePath)
    if (df.isEmpty) 1
    else df.agg(max("ride_id")).as[Int].collect().headOption.getOrElse(0) + 1
  }

  private def insertRideRequests(spark: SparkSession, passengerId: Int, pickupLocId: Long): Int = {
    val deltaTablePath = "data/delta/ride_requests"
    val rideRequestSchema = getRideRequestSchema()
    val colNames = Seq("ride_id", "passenger_id", "driver_id", "pickup_location", "ride_status", "timestamp", "ride_date")
    initializeDeltaTableIfNeeded(spark, deltaTablePath, rideRequestSchema, colNames)
    val now = java.time.Instant.now()
    val today = LocalDate.now()
    val rideId = getNextRideId(spark, deltaTablePath)
    val newRequestDF = spark.createDataFrame(Seq((
        rideId,
        passengerId,
        0,
        pickupLocId,
        "REQUESTED",
        Timestamp.from(now),
        today,
      )))
      .toDF("ride_id", "passenger_id", "driver_id", "pickup_location", "ride_status", "timestamp", "ride_date")
    newRequestDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(deltaTablePath)
    println(s"✅ Ride request written for passenger $passengerId at pickup location $pickupLocId with RRide Id: $rideId")
    rideId
  }

  def assignDriverToRide(
                          spark: SparkSession,
                          deltaTablePath: String,
                          rideId: Int,
                          driverId: Int
                        ): Unit = {
    val deltaTable = DeltaTable.forPath(spark, deltaTablePath)
    deltaTable.updateExpr(
      s"ride_id = $rideId",
      Map("driver_id" -> driverId.toString)
    )
  }

  def updateDriverStatus(
                          spark: SparkSession,
                          deltaTablePath: String,
                          driverId: Int,
                          status: Boolean
                        ): Unit = {
    val deltaTable = DeltaTable.forPath(spark, deltaTablePath)
    deltaTable.updateExpr(
      s"driver_id = $driverId",
      Map("is_available" ->  s"'$status'")
    )
  }

  def updateRideStatus(
                        spark: SparkSession,
                        deltaTablePath: String,
                        rideId: Int,
                        newStatus: String
                      ): Unit = {
    val deltaTable = DeltaTable.forPath(spark, deltaTablePath)
    deltaTable.updateExpr(
      s"ride_id = $rideId",
      Map("ride_status" -> s"'$newStatus'")
    )
  }
}
