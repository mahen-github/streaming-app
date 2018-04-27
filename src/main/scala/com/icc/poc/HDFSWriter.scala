package com.icc.poc

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

class HDFSWriter(val path: String) {
  def writeConsumerRecord(rdd: RDD[ConsumerRecord[String, String]]): RDD[ConsumerRecord[String, String]] = {
    rdd.saveAsTextFile(path)
    rdd
  }

  def writeString(rdd:RDD[String]){
    rdd.saveAsTextFile(path)
  }
}

object HDFSWriter {
  def configure(path: String): HDFSWriter = {
    new HDFSWriter(path)
  }
}