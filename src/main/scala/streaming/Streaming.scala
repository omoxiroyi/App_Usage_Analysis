package streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming extends App {
  val conf = new SparkConf().setAppName("Streaming")

  val ssc = new StreamingContext(conf, Seconds(5))

  ssc.checkpoint("checkpoint")

  val topics = Set("ttt")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "10.0.0.26:9092,10.0.0.30:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean))

  val kafkaStream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams))

  val data = kafkaStream.map(_.value)

  data.print()

  ssc.start
  ssc.awaitTermination
}
