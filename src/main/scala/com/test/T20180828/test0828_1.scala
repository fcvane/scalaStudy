package com.test.T20180828

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign

class test0828_1 {

}

object test0828_1 extends App {
  /**
    *
    */
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val spark = new StreamingContext(conf, Seconds(5))
  val sc = spark.sparkContext
  val kafkaParams = Map(
    // 用于初始化链接到集群的地址
    "bootstrap.servers" -> "bigdata03:9092,bigdata06:9092,bigdata08:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    // 用于标识这个消费者属于哪个消费团体
    "group.id" -> "test",
    // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    // 从最新的开始消费
    "auto.offset.reset" -> "latest",
    // 如果是true，则这个消费者的偏移量会在后台自动提交
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val file = "/tmp/test_4.txt"
  val textFile = sc.textFile(file)
  val fromOffsets = collection.mutable.Map[TopicPartition, Long]()
  fromOffsets += (new TopicPartition("ogg", 0) -> 23.toLong)
  fromOffsets.toMap

  val stream = KafkaUtils.createDirectStream[String, String](
    spark,
    PreferConsistent,
    Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
  )

  stream.foreachRDD { rdd =>
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd.foreachPartition {
      lines => {
        lines.foreach(line => {
          println(line.value())

        })
      }
    }
    // begin your transaction

    // update results
    // update offsets where the end of existing offsets matches the beginning of this batch of offsets
    // assert that offsets were updated correctly

    // end your transaction
  }

}