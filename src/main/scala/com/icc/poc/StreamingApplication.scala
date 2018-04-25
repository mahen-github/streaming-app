package com.icc.poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object StreamingApplication extends App {
  val sparkConf = new SparkConf()
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val ssc = new StreamingContext(sparkContext, Seconds(10))
  val dStream = KafkaReader.configure(ssc).consume()
  val path = "/tmp/test_mahendran"
  val key = dStream.map(record => record.value())
  
  key.window(Seconds(60)).foreachRDD((rdd, time) => {
      rdd.collect().map { x => println(s"""$$$$$$$$$$$$$$$$$$$$$$$$$$ ${time}  :  ${x}""") }
      println(" =======>>>>>>>>>>>>>>>>>>>>>>>>>>> " + rdd.count())
    })
  dStream.foreachRDD((rdd, time) => {
    if (!rdd.isEmpty()) {
      val hdfsWriter = HDFSWriter.configure(path + "/" + time.milliseconds)
      hdfsWriter.writeString(rdd.map(_.value().toString()))
    }
  })
  ssc.start()
  ssc.awaitTermination()
}
