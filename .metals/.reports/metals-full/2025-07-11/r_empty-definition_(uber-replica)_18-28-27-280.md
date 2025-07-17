error id: file://<WORKSPACE>/src/main/scala/RequestRide.scala:
file://<WORKSPACE>/src/main/scala/RequestRide.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1980
uri: file://<WORKSPACE>/src/main/scala/RequestRide.scala
text:
```scala
import GetDriverLocation._
import UpsertDriverCurrLocations.{getRideRequestSchema, initializeDeltaTableIfNeeded}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

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
    val riderequestId: Int = upsertRideRequests(passengerId, pickupLocId)
    getNearestDriver(pickupLocId)
  }

  private def getNearestDriver(pickupLocId: Long): Unit = {
    println(s"[Stub] Would find nearest driver to pickup location $pickupLocId")
  }

  private def upsertRideRequests(passengerId: Int, pickupLocId: Long): Int = {
    val spark = createSparkSessionToReadDeltaFiles()
    val deltaTablePath = "data/delta/ride_requests"
    val rideRequestSchema = getRideRequestSchema()
    val colNames = Seq("passenger_id", "date", "pickup_location", "ride_id", "is_ride_completed", "event_time")

    initializeDeltaTableIfNeeded(spark, deltaTablePath, rideRequestSchema, colNames)

    val now = java.time.Instant.now()
    val today = LocalDate.now().toString
    val newRequestDF = spark.createDataFrame(Seq((
        passengerId,
        rideId,
        today,
        Timestamp.from(now),
        pickupLocId,
        false // is_completed
      )))
      .toDF("passenger_id", "ride_id", "date", "timestamp", "pickup_location")


    newRequestDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(deltaTablePath)

    println(s"✅ Ride request written for passenger $passengerId at pickup location $pickupLocId")
    rideId // Dum@@my ride request ID
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

```


#### Short summary: 

empty definition using pc, found symbol in pc: 