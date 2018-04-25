package consumer
import java.util
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import producer.User

import scala.collection.JavaConverters._

class CustomConsumer {

  /**
    * This method will consumes data from given topic
    * @param topic String
    * @return List[User]
    */
  def consumeFromKafka(topic: String): List[User] = {
    val props = new Properties()
    val config = ConfigFactory.load()


    props.put("bootstrap.servers", config.getString("BOOTSTRAP_SERVER"))
    props.put("key.deserializer", config.getString("DESERIALIZER"))
    props.put("value.deserializer", config.getString("VALUE_DESERIALIZER"))
    props.put("group.id", config.getString("GROUP_ID"))
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getString("OFFSET"))

    val consumer: KafkaConsumer[String, User] = new KafkaConsumer[String, User](props)
    consumer.subscribe(util.Collections.singletonList(topic))
    val record = consumer.poll(5000).asScala.toList.map(_.value())
    record
  }
}


object ConsumerMain extends App {
  val topic = "test-topic"
  (new CustomConsumer).consumeFromKafka(topic)
}
