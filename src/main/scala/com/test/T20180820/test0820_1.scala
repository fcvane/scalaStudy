package com.test.T20180820

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.HashMap

class test0820_1 {

}

object test0820_1 extends App {
  /*
   * 生产者
   * 生产1-100的随机数
   */
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val ssc = new StreamingContext(conf, Seconds(2))
  ssc.checkpoint("checkpoint")

  if (args.length < 4) {
    //<metadataBrokerList> broker地址
    //<topic> topic名称
    //<messagesPerSec> 每秒产生的消息
    //<wordsPerMessage> 每条消息包括的单词数
    System.err.println("Usage: PronName <metadataBrokerList> <topic> <messagesPerSec> <wordsPerMessage>")
    System.exit(1)
  }
  val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

  // 配置节点属性信息props
  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  //通过zookeeper建立kafka的producer
  val producer = new KafkaProducer[String, String](props)

  //通过producer发送一些消息
  while (true) {
    (1 to messagesPerSec.toInt).foreach { messageNum =>
      //连成字符串用空格隔开
      val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
        .mkString(" ")
      //把消息内容和topic封装到ProducerRecord中再发送
      val message = new ProducerRecord[String, String](topic, null, str)
      //发送消息
      producer.send(message)
    }
    //休眠一秒钟
    Thread.sleep(1000)
  }
}