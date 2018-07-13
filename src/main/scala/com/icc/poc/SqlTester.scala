package com.icc.poc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

class SqlTester(@transient val sc: SparkContext) {
  def process( path: String) {
    val stringRdd = sc.textFile(path)
   stringRdd.map(x=> (x.split(",").apply(0), x)).groupByKey().map(x=> {
       (x._1, x._2.map { x => x.split(",").apply(8).toDouble}.reduceLeft(_ + _))
     }  
   ).foreach { x => println(x) }
    stringRdd.foreach { x => println("=====================" + x) }
  }
}

object SqlTester extends App{
  val path = args(0)
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  new SqlTester(sc).process(path)
}