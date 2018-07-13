package com.icc.poc

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata


/**
 * KafkaSink class is a smart wrapper for a Kafka producer. Instead of sending the producer itself, we send only a “recipe” how to create it in an executor.
 * The class is serializable because Kafka producer is initialized just before first use on an executor.
 * Constructor of KafkaSink class takes a function which returns Kafka producer lazily when invoked.
 */

class KafkaSink(createProducer: () => KafkaProducer[Long, Double]) extends Serializable {

  lazy val producer = createProducer()

  //Once the Kafka producer is created, it is assigned to producer variable to avoid initialization on every send() call.
  def send(topic: String, key: Long, value: Double): Future[RecordMetadata] = producer.send(new ProducerRecord(topic, key, value))
}

object KafkaSink {
  def apply(config:java.util.Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[Long, Double](config)

      /**
       * close the Kafka producer before an executor JVM is closed. Without it, all messages buffered internally by Kafka producer would be lost.
       * Register a shutdown hook to be run when the VM exits.
       * The hook is automatically registered: the returned value can be ignored, but is available in case the Thread requires further modification.
       * It can also be unregistered by calling ShutdownHookThread#remove().
       */
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }
}