package com.test.T20180817

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

class test0817_1 {

}

object test0817_1 extends App {
  /*
   *@param sparkSQL操作MySQL
   */
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  //  创建Properties存储数据库相关属性：用户名、 密码
  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "abc123")
  // jdbc链接串
  val url = "jdbc:mysql://127.0.0.1:3306/test0409"
  // 取得该表数据
  // sqlContext.read.jdbc(url,tableName,prop)
  sqlContext.read.jdbc(url, "table0817", prop).show()

  // 通过load获取
  // options函数支持url、driver、dbtable、partitionColumn、lowerBound、upperBound以及numPartitions选项
  val jdbcDF = sqlContext.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://127.0.0.1:3306/test0409",
      "user" -> "root",
      "password" -> "abc123",
      "dbtable" -> "table0817")).load()
  jdbcDF.registerTempTable("tt")
  sqlContext.sql("select * from tt where id > 20").show()
  //停止SparkContext
  sc.stop()
}