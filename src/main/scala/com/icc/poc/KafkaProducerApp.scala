package com.icc.poc

import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

class KafkaProducerApp {

  def write() {
    val props: Map[String, Any] = Map(
      "bootstrap.servers" -> "node1:9092",
      "acks" -> "all",
      "batch.size" -> 16384,
      "linger.ms" -> 100,
      "buffer.memory" -> 33554432,
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "com.icc.poc.DataSerializer")
    val kafkaParams = new Properties()
    props.map { f => kafkaParams.put(f._1, f._2.toString()) }
    val producer = new KafkaProducer[String, Data](kafkaParams)
    val it = ClassLoader.getSystemResourceAsStream("sample_cc.csv")
    val br = new BufferedReader(new InputStreamReader(it));
    var line = br.readLine()
    while (line != null) {
      val tokens = line.split(",")
      val data: Data = Data(tokens(0).toLong, tokens(1).toLong, tokens(2), tokens(3).toLong, tokens(4), tokens(5), tokens(6).toLong, tokens(7), tokens(8).toDouble, tokens(9).toShort)
      val f = producer.send(new ProducerRecord[String, Data]("edhTopic", data))
      line = br.readLine()
    }
    producer.close()
  }
}

object KafkaProducerApp extends App {
  val writer = new KafkaProducerApp()
  writer.write()
}