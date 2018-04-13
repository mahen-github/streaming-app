package com.icc.poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object StreamingApplication extends App {
  val sparkConf = new SparkConf()
  val sparkContext = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sparkContext, Seconds(25))
  val dStream = KafkaReader.configure(ssc).consume()
  val path = "/tmp/test_mahendran"
  
  dStream.foreachRDD((rdd, time) => {
    if (!rdd.isEmpty()) {
      val hdfsWriter = HDFSWriter.configure(path + "/" + time.milliseconds)
      hdfsWriter.writeConsumerRecord(rdd)
    }
  })
  ssc.start()
  ssc.awaitTermination()
}
/**
 * su - hdfs -c "spark-submit --name Mahendran --class com.icc.poc.StreamingApplication 
  --master yarn --deploy-mode cluster --queue edhops streaming-app-1.0.1-SNAPSHOT.jar" 
 */

