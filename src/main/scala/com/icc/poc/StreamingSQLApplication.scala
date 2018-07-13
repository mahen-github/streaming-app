package com.icc.poc

import scala.reflect.runtime.universe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.Row
import org.apache.spark.streaming.kafka010.HasOffsetRanges

object StreamingSQLApplication extends App {
  val sparkConf = new SparkConf()
  val sparkContext = new SparkContext(sparkConf)

  val ssc = new StreamingContext(sparkContext, Seconds(20))
  val dStream = KafkaReader.configure(ssc).consume()
  val path = "/tmp/test_mahendran"
  val kafkaWriter = KafkaWriter.configure(sparkContext)

  val spark = SparkSession
    .builder().config(sparkConf).appName("Mahendran-SQLStreaming-APP")
    .getOrCreate()

    //To convert RDD to DFs
  import spark.implicits._

  /**
   * Modify it late
   */
  spark.conf.set("spark.sql.shuffle.partitions", 6)
  spark.conf.set("spark.executor.memory", "2g")

  import org.apache.spark.sql._
  val x = dStream.map(record => record.value()).window(Seconds(60)).foreachRDD((rdd, time) => {
    KafkaUtilService.configure().setData(rdd)
    val reducedData = rdd.map { data =>
      {
        val tokens = data.split(",")
        val transData = trans(tokens(0).toLong, tokens(1).toLong, tokens(2), tokens(3).toLong, tokens(4), tokens(5), tokens(6).toLong, tokens(7), tokens(8).toDouble, tokens(9).toShort)
        Row(transData.customer_id, transData.acct_num, transData.customer_profile, transData.trans_num, transData.trans_date, transData.trans_time, transData.unix_time, transData.category, transData.amt, transData.is_fraud)
      }
    }

    //Map a RDD[ROW] to a class to get schema
    val transSchema = Encoders.product[trans].schema
    reducedData.cache()

    val transSchemaRDD = spark.sqlContext.createDataFrame(reducedData, transSchema)
    transSchemaRDD.createOrReplaceTempView("transTable")
    val result = spark.sql("select customer_id, SUM(amt) as total from transTable group by customer_id")
    println("=================================== " + time)
    result.printSchema()

    result.collect().foreach(println(_))

    result.where(result("total") > 1000).collect().foreach { x => println("Filtered val : " + x) }

    println("COUNT ::::::::::::::::::::::: " + result.count())
    
    reducedData.collect().foreach(println)

    val resultRDD = result.where(result("total") > 1000).map { x => (x.getLong(0), x.getDouble(1)) }.rdd

    resultRDD.collect().foreach(x => println("Result RDD => " + (x._1 , x._2) ))

    println("=================================== ")
    
    kafkaWriter.write(resultRDD)

    //        Write the original data into hdfs
    if (!rdd.isEmpty()) {
      val hdfsWriter = HDFSWriter.configure(path + "/actualData/" + time.milliseconds)
      hdfsWriter.writeString(rdd)
    }
  })

  ssc.start()
  ssc.awaitTermination()
}
