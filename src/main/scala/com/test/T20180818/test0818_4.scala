package com.test.T20180818

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class test0818_4 {

}

object test0818_4 extends App {
  /*
   *@param kudu操作  -sparkSQL
   */
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  // 读取数据
  val sqlContext = new SQLContext(sc)
  // kudu信息
  val df = sqlContext.read.options(
    Map(
      "kudu.master" -> "bigdata02:7051,bigdata03:7051,bigdata04:7051,bigdata05:7051,bigdata06:7051,bigdata07:7051,bigdata08:7051",
      "kudu.table" -> "test0818"
    )).kudu
  // 注册临时表
  df.registerTempTable("tt")
  val filteredDF = sqlContext.sql("select * from tt where age >= 20").show()

}