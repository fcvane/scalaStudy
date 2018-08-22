package com.test.T20180820

import java.io.{File, FileWriter}

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, TaskContext, streaming}

class test0820_3 {

}

object test0820_3 extends App {
  /*
   *消费信息 断点消费、保存offset
   */
  val zk = test0820_4
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val ssc = new StreamingContext(conf, streaming.Seconds(5))
  // spark参数信息(可在spark-submit执行命令行添加)
  conf.set("spark.streaming.stopGracefullyOnShutdown", "true") //优雅的关闭
  conf.set("spark.streaming.backpressure.enabled", "true") //激活削峰功能
  conf.set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值
  conf.set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
  // 消费主题
  val topic = "test0820"
  val topics = Array(topic)
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
  //   val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
  /*
   * 判断zk中是否有保存过该计算的偏移量
   * 如果没有保存过,使用不带偏移量的计算,在计算完后保存
   * 精髓就在于KafkaUtils.createDirectStream这个地方
   * 默认是KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))，不加偏移度参数
   */
  val stream = if (zk.znodeIsExists(s"${topic}offset")) {
    val nor = zk.znodeDataGet(s"${topic}offset")
    //创建topic，分区为k 偏移度为v的map
    val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
    println(s"topic ${nor(0).toString}")
    println(s"Partition ${nor(1).toInt}")
    println(s"offset ${nor(2).toLong}")
    println(s"zk中取出来的kafka偏移量 $newOffset")
    KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, newOffset))
  } else {
    println(s"第一次计算,没有zk偏移量文件")
    println(s"手动创建一个偏移量文件 ${topic}offset 默认从0偏移度开始计算")
    zk.znodeCreate(s"${topic}offset", s"$topic, 0")
    val nor = zk.znodeDataGet(s"${topic}offset")
    val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
    KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, newOffset))
  }
  stream.map(record => (record.key, record.value)).print()
  //zk的路径
  //val zkOffsetPath = "/sparkstreaming/20180820"
  // 存储偏移量
  stream.foreachRDD {
    rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // // 获取偏移量
      rdd.foreachPartition {
        iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"topic ${o.topic}")
          println(s"Partition ${o.partition}")
          println(s"Fromoffset ${o.fromOffset}")
          println(s"Utiloffset ${o.untilOffset}")
          // 写zookeeper
          zk.offsetWork(s"${o.topic}offset", s"${o.topic},${o.partition},${o.untilOffset}")

          // 写本地文件系统
          val fw = new FileWriter(new File("./files/offset.log"), true)
          fw.write(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset} \n")
          fw.close()
      }
  }

  ssc.start()
  ssc.awaitTermination()
}
