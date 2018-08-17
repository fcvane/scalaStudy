package com.test.T20180817

import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

class test0817_2 {

}

object test0817_2 extends App {
  /*
   *@param 读取数据写入MySQL
   */
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  // 创建RDD
  val dataRDD = sc.parallelize(Array("1,A,20", "2,B,30", "3,C,25")).map(_.split(','))
  // 通过StructType直接指定每个字段的schema
  val schema = StructType(
    List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    )
  )
  // 将RDD映射到rowRDD
  val rowRDD = dataRDD.map(x => Row(x(0).toInt, x(1).trim, x(2).toInt))
  // 将schema信息应用到rowRDD上
  val dataFrame = sqlContext.createDataFrame(rowRDD, schema)
  // 创建Properties存储数据库相关属性
  val url = "jdbc:mysql://127.0.0.1:3306/test"
  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "abc123")
  // 将数据覆盖写入到数据库
  dataFrame.write.mode(SaveMode.Overwrite).jdbc(url, "table0817", prop)
  // 查询
  sqlContext.read.jdbc(url, "table0817", prop).show()
  //停止SparkContext
  sc.stop()
}
