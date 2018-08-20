package com.test.T20180820

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

class test0820_2 {

}

object test0820_2 extends App {
  /*
   * 消费kafka数据
   */
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  // 创建streamingContext
  val ssc = new StreamingContext(conf, Seconds(2))
  // 消费主题
  val topics = "test0820"
  // 消费者配置
  val kafkaParams = Map(
    // 用于初始化链接到集群的地址
    "bootstrap.servers" -> "bigdata03:9092,bigdata06:9092,bigdata08:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    // 用于标识这个消费者属于哪个消费团体
    "group.id" -> "test",
    // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    // 可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    // 如果是true，则这个消费者的偏移量会在后台自动提交
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  // 创建DStream，返回接收到的输入数据
  val topicsSet = topics.split(",").toSet
  var stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topicsSet, kafkaParams))
  // 每一个stream都是一个ConsumerRecord
  stream.map(_.value).map(x => (x, 1)).reduceByKey(_ + _).print()
  stream.map(record => (record.key, record.value)).print()
  // 开始工作
  ssc.start()
  ssc.awaitTermination()

}