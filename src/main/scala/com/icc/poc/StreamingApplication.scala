package com.icc.poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

case class trans(customer_id: Long, acct_num: Long, customer_profile: String,
  trans_num: Long, trans_date: String, trans_time: String, unix_time: Long, category: String, amt: Double, is_fraud: Short)

object StreamingApplication extends App {
  val sparkConf = new SparkConf()
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val ssc = new StreamingContext(sparkContext, Seconds(20))
  val dStream = KafkaReader.configure(ssc).consume()
  val path = "/tmp/test_mahendran"
  val kafkaWriter = KafkaWriter.configure(sparkContext)
  val x = dStream.map(record => record.value()).window(Seconds(60), Seconds(20)).foreachRDD((rdd, time) => {
    val reducedData = rdd.map { data =>
      {
        val tokens = data.split(",")
        val transData = trans(tokens(0).toLong, tokens(1).toLong, tokens(2), tokens(3).toLong, tokens(4), tokens(5), tokens(6).toLong, tokens(7), tokens(8).toDouble, tokens(9).toShort)
        (transData.customer_id, transData.amt)
      }
    }.reduceByKey {
      (a, b) => (a + b)
    }
    println("=================================== " + time)
    reducedData.collect().foreach(println)
    println("=================================== ")
    kafkaWriter.write(reducedData)

    //Write the original data into hdfs
    if (!rdd.isEmpty()) {
      val hdfsWriter = HDFSWriter.configure(path + "/actualData/" + time.milliseconds)
      hdfsWriter.writeString(rdd)
    }
  })
  
  ssc.start()
  ssc.awaitTermination()
}
