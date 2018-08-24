package com.test.T20180823

import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

class test0823_1 {

}

object test0823_1 extends App{
  /**
    *
    */
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  // kudu信息
  val df = hiveContext.read.options(
    Map(
      "kudu.master" -> "bigdata02:7051,bigdata03:7051,bigdata04:7051,bigdata05:7051,bigdata06:7051,bigdata07:7051,bigdata08:7051",
      "kudu.table" -> "test0818"
    )).kudu
  // 注册临时表
  df.registerTempTable("tt")
  hiveContext.sql("select * from tt where id in (select 1 from test_db.dual)").show()
  hiveContext.sql("select 1 from test_db.dual").show()
}
