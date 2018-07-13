package com.icc.poc

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010.OffsetRange
import scala.collection.concurrent.TrieMap
import org.apache.spark.streaming.Time
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaReader(val ssc: StreamingContext) {
  var timestamp: Long = _

  val processing = ssc.sparkContext.accumulableCollection(TrieMap[Time, Array[OffsetRange]]())
  var kD: InputDStream[ConsumerRecord[String, String]] = _

  def consume() = {
    val kafkaParams = Map(
      "bootstrap.servers" -> Constants.BOOTSTRAP_SERVERS,
      "auto.offset.reset" -> Constants.KAFKA_RESET,
      "group.id" -> KafkaReader.GROUP_ID,
      "fetch.message.max.bytes" -> Constants.KAFKA_MAX_FETCH_BYTES,
      "key.deserializer" -> classOf[StringDeserializer],
      //      "value.deserializer" -> classOf[com.icc.poc.DataDeserializer])
      "value.deserializer" -> classOf[StringDeserializer])
    val topics = KafkaReader.KAFKA_READ_TOPICS
//    val offsets = KafkaUtilService.configure().getOffsets().getOrElse(Map.empty)
//    if (!offsets.isEmpty) {
//      val y = offsets map { case (k, v) => (new TopicPartition("edhTopic", k), v) }
//      kD = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, y))
//    } else {
//      kD = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
//    }
    kD
  }
}

object KafkaReader {
  final val KAFKA_READ_TOPICS = Set("edhTopic")
  final val GROUP_ID = "Mahendran"
  def configure(ssc: StreamingContext): KafkaReader = {
    new KafkaReader(ssc)
  }
}
