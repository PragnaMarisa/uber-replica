package com.uberreplica.driver

import com.uberreplica.customDelta.CustomDeltaUtils.createDeltaSparkSession
import com.uberreplica.driver.GetDetails.{createSparkSessionToReadDeltaFiles, loadDeltaTable}
import com.uberreplica.ride.RequestRide.{updateDriverStatus, updateRideStatus}

object CompleteRide {

  def main(args:Array[String]): Unit = {
    val rideRequestId: Int = args(0).toInt
    val driverId = getDriverIdOfRide(rideRequestId);
    val spark = createDeltaSparkSession()
    updateRideStatus(spark, "data/delta/ride_requests", rideRequestId, "COMPLETED")
    updateDriverStatus(spark, "data/delta/driver_current_locations", driverId, "false")
  }

  private def getDriverIdOfRide(rideRequestId: Int):Int = {
    val spark = createSparkSessionToReadDeltaFiles();
    val data = loadDeltaTable(spark, "data/delta/ride_requests")
    data.filter(data("ride_id") === rideRequestId)
      .select("driver_id")
      .head()
      .getInt(0)
  }

}
