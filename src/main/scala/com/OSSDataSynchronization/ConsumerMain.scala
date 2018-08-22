package com.OSSDataSynchronization

import java.io.{File, FileWriter}
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}

class ConsumerMain {

}

object ConsumerMain extends App {

  val zk = ZookeeperManager
  val kf = KafkaOffsetManager
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val ssc = new StreamingContext(conf, spark.streaming.Seconds(5))
  // spark参数信息(可在spark-submit执行命令行添加)
  conf.set("spark.streaming.stopGracefullyOnShutdown", "true") //优雅的关闭
  conf.set("spark.streaming.backpressure.enabled", "true") //激活削峰功能
  conf.set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值
  conf.set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
  // 消费主题
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))
  val topics = properties.getProperty("kafka.topic").split(",").toSet
  val group = "test"
  // 消费者配置
  val kafkaParams = Map(
    // 用于初始化链接到集群的地址
    "bootstrap.servers" -> "bigdata03:9092,bigdata06:9092,bigdata08:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    // 用于标识这个消费者属于哪个消费团体
    "group.id" -> group,
    // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    // 从最新的开始消费
    "auto.offset.reset" -> "latest",
    // 如果是true，则这个消费者的偏移量会在后台自动提交
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val stream = kf.createDirectStream(ssc, kafkaParams, topics)
  stream.foreachRDD {
    rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // // 获取偏移量
      rdd.foreachPartition {
        iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"[ ConsumerMain ] topic: ${o.topic}; partition: ${o.partition}; fromoffset: ${o.fromOffset}; utiloffset: ${o.untilOffset}")
          // 写zookeeper
          zk.zkSaveOffset(s"${o.topic}offset", s"${o.partition}", s"${o.topic},${o.partition},${o.untilOffset}")
          // 关闭
          zk.zooKeeper.close()
          // 写本地文件系统
          val fw = new FileWriter(new File("./files/offset.log"), true)
          fw.write(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} \n")
          fw.close()
          // 新版本Kafka自身保存
          stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
  }
  ssc.start()
  ssc.awaitTermination()
}
