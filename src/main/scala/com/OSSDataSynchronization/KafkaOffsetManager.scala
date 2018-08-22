package com.OSSDataSynchronization

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, LocationStrategies}

class KafkaOffsetManager {

}

object KafkaOffsetManager {
  /**
    * 读取和保存Kafka偏移量 (Kafka0.10 and higher)
    */

  // lazy声明的变量只有在使用的时候才会进行初始化 --惰性变量
  // lazy val log = org.apache.log4j.LogManager.getLogger("KafkaOffsetManager")
  val zk = ZookeeperManager

  /** 读取zk中偏移量，如果有就返回对应的分区和偏移量
    * 如果没有就返回None
    *
    * @param zkOffsetPath 偏移量路径
    * @param topic        topic名字
    * @return 偏移量Map or None
    */
  def zkReadOffset(zkOffsetPath: String, topic: String): Option[Map[TopicPartition, Long]] = {
    if (zk.znodeIsExists(zkOffsetPath)) {
      val nor = zk.znodeDataGet(zkOffsetPath)
      //创建topic，分区为k 偏移度为v的map 分区序号1:偏移量1,分区序号2:偏移量2,......
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
      println("[ KafkaOffsetManager ] --------------------------------------------------------------------")
      println(s"[ KafkaOffsetManager ] topic ${nor(0).toString}")
      println(s"[ KafkaOffsetManager ] Partition ${nor(1).toInt}")
      println(s"[ KafkaOffsetManager ] offset ${nor(2).toLong}")
      println(s"[ KafkaOffsetManager ] zk中取出来的kafka偏移量 $newOffset")
      println("[ KafkaOffsetManager ] --------------------------------------------------------------------")
      Some(newOffset)
    }
    else {
      println("[ KafkaOffsetManager ] --------------------------------------------------------------------")
      println("[ KafkaOffsetManager ] 第一次计算,没有zk偏移量文件")
      println(s"[ KafkaOffsetManager ] 手动创建一个偏移量文件 ${topic}offset 默认从0偏移度开始计算")
      println("[ KafkaOffsetManager ] --------------------------------------------------------------------")
      zk.znodeCreate(zkOffsetPath, s"$topic, 0")
      val nor = zk.znodeDataGet(zkOffsetPath)
      val newOffset = Map(new TopicPartition(nor(0).toString, nor(1).toInt) -> nor(2).toLong)
      Some(newOffset)
    }
  }

  /** 保存每个批次的rdd的offset到zk中
    *
    * @param zkOffsetPath 偏移量路径
    * @param rdd          每个批次的rdd
    */
  def zkSaveOffset(zkOffsetPath: String, rdd: RDD[_]): Unit = {
    //转换rdd为Array[OffsetRange]
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //转换每个OffsetRange为存储到zk时的字符串格式 :  分区序号1:偏移量1,分区序号2:偏移量2,......
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}").mkString(",")
    //将最终的字符串结果保存到zk里面
    zk.znodeCreate(zkOffsetPath, offsetsRangesStr)
  }

  /** 创建数据流
    *
    * @param ssc         程序主入口
    * @param kafkaParams kafka配置
    * @param topics      消费主题
    * @return kafkaStream 返回创建的kafkaStream
    */
  def createDirectStream(ssc: StreamingContext,
                         kafkaParams: Map[String, Object],
                         topics: Set[String]): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, org.apache.spark.streaming.kafka010.ConsumerStrategies
        .Subscribe[String, String](topics, kafkaParams))
    kafkaStream
  }


def main (args: Array[String] ): Unit = {
  zkReadOffset ("test0820offset", "test0820")
}

}