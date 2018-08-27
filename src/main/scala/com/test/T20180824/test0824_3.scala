package com.test.T20180824

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class test0824_3 {

}

object test0824_3 extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  //val spark = SparkSession.builder().config(conf).getOrCreate()
  //val sc = spark.sparkContext

  val str = "{\"table\":\"OGG.STEST1\",\"op_type\":\"U\",\"op_ts\":\"2018-08-25 07:28:15.343159\",\"current_ts\":\"2018-08-25T15:28:20.639000\",\"pos\":\"00000000000000003047\",\"before\":{\"ID\":1,\"NAME\":\"A\"},\"after\":{\"id\":1,\"name\":\"A\"}}"
  val json = JSON.parseObject(str)

  val data = json.getJSONObject("after")
  //  println(data + "1")
  val tableName = json.getString("table").split("\\.")(1).toLowerCase()
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))
  val kuduMaster = properties.getProperty("kudu.master")
  val kuduContext = new KuduContext(kuduMaster, sc)
  // 使用nil做定界符
  val rdd = sc.makeRDD(data.toString.stripMargin :: Nil)
  val df = sqlContext.read.json(rdd)
  df.show()

  //  kuduContext.upsertRows(df, tableName)

}