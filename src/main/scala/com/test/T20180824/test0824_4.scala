package com.test.T20180824

import java.util
import java.util.Properties

import org.apache.kudu.Type
import org.apache.kudu.client.KuduClient
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

class test0824_4 {

}

object test0824_4 extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val str = "{\"table\":\"OGG.STEST1\",\"op_type\":\"U\",\"op_ts\":\"2018-08-25 07:28:15.343159\",\"current_ts\":\"2018-08-25T15:28:20.639000\",\"pos\":\"00000000000000003047\",\"before\":{\"ID\":1,\"NAME\":\"A\"},\"after\":{\"ID\":1,\"NAME\":\"A\"}}"

  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))
  val kuduMaster = properties.getProperty("kudu.master")
  val kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
  val result = new util.HashMap[String, Any]()
  val jsonObj = JSON.parseFull(str)
  val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
  // 需要加载的值
  val data: Map[String, Any] = map.get("after").get.asInstanceOf[Map[String, Any]]
  val opType = map.get("op_type")
  val tableName = map.get("table").get.asInstanceOf[String].split("\\.")(1).toLowerCase
  tableName match {
    case null =>
      println("[ KuduManager ] json is abnormal where not exists tablename ")
      None
    case _ =>
      val kuduTable = kuduClient.openTable(tableName)
      val schema = kuduTable.getSchema
      val list = new ListBuffer[String]

      // 字段赋值
      for (i <- 0 until schema.getColumns.size) {
        val colSchema = schema.getColumnByIndex(i)
        val colName = colSchema.getName.toUpperCase
        val colType: Type = colSchema.getType
        if (data.get(colName) != None) {
          colType match {
            case Type.STRING =>
              try {
                result.put(colName.toLowerCase, data.get(colName).get.asInstanceOf[String])
              } catch {
                case _ =>
                  result.put(colName.toLowerCase, data.get(colName).get.asInstanceOf[Number].intValue())
              }
            case Type.DOUBLE =>
              result.put(colName.toLowerCase, data.get(colName).get.asInstanceOf[Double])
            case Type.FLOAT =>
              result.put(colName.toLowerCase, data.get(colName).get.asInstanceOf[Float])
            case Type.UNIXTIME_MICROS =>
              result.put(colName.toLowerCase, data.get(colName).get.asInstanceOf[UnixTimestamp])
            case _ =>
              result.put(colName.toLowerCase, data.get(colName).get.asInstanceOf[Int])
          }
        }
        // 状态 delete_state 字段赋值
        opType.toString match {
          case "D" =>
            result.put("delete_state", "1")
          case _ =>
            result.put("delete_state", "0")
        }
      }
  }
  val values = result.values().toArray().reverse.mkString(",")
  println(values)
  //sqlContext.sql(s"insert into table ${tableName} values (${values})")
  kuduClient.close()
}