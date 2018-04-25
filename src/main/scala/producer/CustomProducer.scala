package producer

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


case class User(name:String,id:Int)

class CustomProducer{
  /**
    * This method will write data to given topic
    * @param topic : String
    */
  def writeToKafka(topic:String): Unit = {
    val props = new Properties()
    val config = ConfigFactory.load()

    props.put("bootstrap.servers", config.getString("BOOTSTRAP_SERVER"))
    props.put("key.serializer", config.getString("SERIALIZER"))
    props.put("value.serializer", config.getString("VALUE_SERIALIZER"))

    val producer = new KafkaProducer[String,User](props)
    val message = "Kafka"
    for(i <- 1 to 30) {
      val record = new ProducerRecord[String, User](topic,User(message + i, i))
      producer.send(record)
    }
  }
}

object ProducerMain extends App{
  val topic = "test-topic"
  (new CustomProducer).writeToKafka(topic)
}

