package com.uberreplica.driver

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

case class Driver(id: Int, var location: Int, isAvailable: Boolean = true)

object ProduceLocations {
  val numDrivers = 100
  val startGap = 10
  val topic = "driver_status"

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)


  def main(args: Array[String]): Unit = {
    val drivers = (0 until numDrivers).map(i => Driver(i, i * startGap)).toArray

    while (true) {
      drivers.foreach { driver =>
        driver.location += 1
        val eventTime = java.time.Instant.now().toString
        val message = s"""{"driver_id":${driver.id},"driver_location":${driver.location}, "is_available":${driver.isAvailable}, "event_time":"$eventTime"}"""
        producer.send(new ProducerRecord[String, String](topic, driver.id.toString, message))
      }
      Thread.sleep(5000)
    }
  }
}