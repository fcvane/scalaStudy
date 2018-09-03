package com.test.T20180903

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkTraining {

}

object SparkTraining extends App {
  /**
    * 巩固练习
    * nc -lp 9999
    * ./bin/run-example streaming.NetworkWordCount localhost 9999
    */
  val isLocal = true
  val sparkSession = if (isLocal) {
    SparkSession.builder
      .master("local[2]")
      .appName("SparkTraining")
      .enableHiveSupport()
      .getOrCreate()
  } else {
    SparkSession.builder
      .appName("SparkTraining")
      .enableHiveSupport()
      .getOrCreate()
  }
  val host = args(0)
  val port = args(1)
  val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(2))
  //  val lines = ssc.sparkContext.textFile("hdfs://bigdata02:8020/tmp/data.txt")
  val lines = ssc.socketTextStream(host, port.toInt)
  val words = lines.flatMap(line => line.toLowerCase.split(" "))
  val wordCounts = words.map(word => (word, 1))
    .reduceByKey((a, b) => a + b)

  wordCounts.foreachRDD(rdd => {
    println("{")
    val localCollection = rdd.collect()
    println("  size:" + localCollection.length)
    localCollection.foreach(r => println("  " + r))
    println("}")
  })


  ssc.start()
  ssc.awaitTermination()

}