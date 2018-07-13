package com.icc.poc

import java.util.Properties

import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.DoubleSerializer

class KafkaWriter(@transient val sparkContext: SparkContext, val kafkaSink: KafkaSink) extends Logging with Serializable {
  def write(rdd: RDD[(Long, Double)]) {
    /**
     * Connection pooling : Yet to be implemented
     */
    val kafkaProducer = sparkContext.broadcast(kafkaSink)

    rdd.foreachPartition { dataIterator =>
      {
        //Don't create a producer for every partition.
        //Create a kafkaSink for the below reason.
        //val producer = new KafkaProducer[Long, Double](kafkaParams)

        /**
         * https://allegro.tech/2015/08/spark-kafka-integration.html
         * Kafka producer is created and closed on an executor and does not need to be serialized.
         * But it does not scale at all, the producer is created and closed for every single message.
         * Establishing a connection to the cluster takes time.
         * It is a much more time consuming operation than opening plain socket connection,
         *  as Kafka producer needs to discover leaders for all partitions.
         *  Kafka Producer itself is a “heavy” object, so you can also expect high CPU utilization by the JVM garbage collector.
         */
        dataIterator.foreach { f =>
          try {
            val future = kafkaProducer.value.send("preBMVTopic", f._1, f._2)
            val result: RecordMetadata = future.get()
            log.info(s"""Written data to the partition ${result.partition()}  =>  offset : ${result.offset()}""")
          } catch {
            case e: Exception =>
              log.error("Exception caught sending to Kafka :", e)
          }
        }
      }
    }
  }
}

object KafkaWriter {
  val props: Map[String, Any] = Map(
    "bootstrap.servers" -> Constants.BOOTSTRAP_SERVERS,
    "acks" -> Constants.ACKS,
    "batch.size" -> Constants.BATCH_SIZE,
    "linger.ms" ->  Constants.LINGER_MS,
    "buffer.memory" -> Constants.BUFFER_MEMORY,
    "key.serializer" -> classOf[LongSerializer],
    "value.serializer" -> classOf[DoubleSerializer])
  val kafkaParams = new Properties()
  props.map { f => kafkaParams.put(f._1, f._2.toString()) }

  def configure(sparkContext: SparkContext): KafkaWriter = {
    val kafkaSink = KafkaSink(kafkaParams)
    new KafkaWriter(sparkContext, kafkaSink)
  }
}