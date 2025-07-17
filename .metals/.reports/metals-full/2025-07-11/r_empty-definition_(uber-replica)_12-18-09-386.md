error id: file://<WORKSPACE>/src/main/scala/ProduceDriverLocations.scala:
file://<WORKSPACE>/src/main/scala/ProduceDriverLocations.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 335
uri: file://<WORKSPACE>/src/main/scala/ProduceDriverLocations.scala
text:
```scala
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
case class Driver(id: Int, var location: Int, isAvailable: Boolean = true)

object ProduceDriverLocations {
  val numDrivers = 100
  val startGap = 10
  val topic = "driver_status"

  val props = new Properties()
  props.put("bootstra@@p.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  def main(args: Array[String]): Unit = {
    val drivers = (0 until numDrivers).map(i => Driver(i, i * startGap)).toArray

    while (true) {
      drivers.foreach { driver =>
        driver.location += 1
        val message = s"""{"driver_id":${driver.id},"driver_location":${driver.location}, "is_available":${driver.isAvailable}}"""
        producer.send(new ProducerRecord[String, String](topic, driver.id.toString, message))
      }
      Thread.sleep(1000)
    }
//     producer.close() // Unreachable, but should be called on shutdown
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 