package com.test.T20180824

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.kudu.client.KuduClient
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks.break

class test0824_2 {

}

object test0824_2 extends App {
  val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Test"))
  val sparksql = new SQLContext(sc)

  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))
  val kuduMaster = properties.getProperty("kudu.master")
  //val kuduContext = new KuduContext(kuduMaster, sc)

  val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
  val newSession = client.newSession()
  val openTable = client.openTable("stest1")
  val schema = openTable.getSchema
  println(schema.getColumns.size())
  println(schema.getColumns)

  for (i <- 0 until schema.getColumns.size()) {
    val columnSchemal = schema.getColumnByIndex(i)
    val colName = columnSchemal.getName
    println(colName)
  }

  val str = "{\"table\":\"OGG.STEST1\",\"op_type\":\"I\",\"op_ts\":\"2018-08-24 07:02:17.115348\",\"current_ts\":\"2018-08-24T15:02:23.497000\",\"pos\":\"00000000000000002300\",\"after\":{\"ID\":2,\"NAME\":\"C\"}}"
  val json = JSON.parseObject(str)
  //返回字符串成员
  val tableName = json.getString("table")
  //匹配判断
  tableName match {
    case null =>
      println("2222")
      break
    case _ =>
      println("1111")
      println(client.tableExists(tableName))
      val kuduTable = client.openTable(json.getString("table").split("\\.")(1).toLowerCase())
      val schema = kuduTable.getSchema
      for (i <- 0 until schema.getColumns.size()) {
        val colName = schema.getColumnByIndex(i).getName
        println(colName.toUpperCase)
      }
  }
}