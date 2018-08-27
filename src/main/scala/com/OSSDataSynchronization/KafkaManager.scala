package com.OSSDataSynchronization

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kudu.client.KuduClient
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import scala.util.control.Breaks._

class KafkaManager {

}

object KafkaManager {
  /**
    * 读取和保存Kafka偏移量 (Kafka0.10 and higher)
    */

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

  /**
    * Kafka解析消费数据入库
    *
    * @param kuduClient kudu客户端
    * @param line       Kafka消费数据(Json类型)
    */
  def dataParseJson(kuduClient: KuduClient, line: String): Unit = {
    val json = JSON.parseObject(line)
    //返回字符串成员：由于首次消费将获取表结构信息，需要过滤处理
    //返回字符串成员
    val tableName = json.getString("table")
    //匹配判断
    tableName match {
      case null =>
        println("2222")
        break
      case _ =>
        println("1111")
        println(kuduClient.tableExists(tableName))
        val kuduTable = kuduClient.openTable(json.getString("table").split("\\.")(1).toLowerCase())
        val schema = kuduTable.getSchema
        for (i <- 0 until schema.getColumns.size()) {
          val colName = schema.getColumnByIndex(i).getName
          println(colName.toUpperCase)
        }
    }
  }

}