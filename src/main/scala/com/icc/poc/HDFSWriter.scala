package com.icc.poc
/**
 * @author Mahendran Ponnusamy
 */
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD

class HDFSWriter(val path: String) {
  def writeConsumerRecord(rdd: RDD[ConsumerRecord[String, String]]): RDD[ConsumerRecord[String, String]] = {
    rdd.saveAsTextFile(path)
    rdd
  }

  def writeString(rdd:RDD[String]): RDD[String]={
    rdd.saveAsTextFile(path)
    rdd
  }
}

object HDFSWriter {
  def configure(path: String): HDFSWriter = {
    new HDFSWriter(path)
  }
  //TBI: get Host FS Conf
}