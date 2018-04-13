package com.icc.poc

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.common.TopicPartition

class KafkaReader(val ssc: StreamingContext) {
  var timestamp: Long = _
  final val KAFKA_BROKERS = "node1:9092,node2:9092,node3:9092"
  final val KAFKA_RESET = "earliest"
  final val KAFKA_MAX_FETCH_BYTES = "16000"
  final val KAFKA_READ_TOPICS = Set("edhTopic")
  final val GROUP_ID = "Mahendran"
  def consume() = {
    val kafkaParams = Map(
      "bootstrap.servers" -> KAFKA_BROKERS,
      "auto.offset.reset" -> KAFKA_RESET,
      "group.id" -> GROUP_ID,
      "fetch.message.max.bytes" -> KAFKA_MAX_FETCH_BYTES,
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer])
    val topics = KAFKA_READ_TOPICS
    val offsets = Map(new TopicPartition("topic3", 0) -> 2L)
    KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
  }
}

object KafkaReader {
  def configure(ssc: StreamingContext): KafkaReader = {
    new KafkaReader(ssc)
  }
}
