package com.icc.poc

/**
 * @author Mahendran Ponnusamy
 */
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongSerializer

class KafkaProducerApp {

  def write() {
    val producer = new KafkaProducer[String, Data](KafkaProducerApp.kafkaParams)
    val it = ClassLoader.getSystemResourceAsStream("sample_cc.csv")
    val br = new BufferedReader(new InputStreamReader(it));
    var line = br.readLine()
    while (line != null) {
      val tokens = line.split(",")
      val data: Data = Data(tokens(0).toLong, tokens(1).toLong, tokens(2), tokens(3).toLong, tokens(4), tokens(5), tokens(6).toLong, tokens(7), tokens(8).toDouble, tokens(9).toShort)
      val f = producer.send(new ProducerRecord[String, Data](KafkaProducerApp.TOPIC, data))
      line = br.readLine()
    }
    producer.close()
  }
}

object KafkaProducerApp extends App {
  final val TOPIC = "edhTopic"
  val props: Map[String, Any] = Map(
    "bootstrap.servers" -> Constants.BOOTSTRAP_SERVERS,
    "acks" -> Constants.ACKS,
    "batch.size" -> Constants.BATCH_SIZE,
    "linger.ms" -> Constants.LINGER_MS,
    "buffer.memory" -> Constants.BUFFER_MEMORY,
    "key.serializer" -> classOf[LongSerializer],
    "value.serializer" -> classOf[DataSerializer])
  val kafkaParams = new Properties()
  props.map { f => kafkaParams.put(f._1, f._2.toString()) }
  val writer = new KafkaProducerApp()
  writer.write()
}