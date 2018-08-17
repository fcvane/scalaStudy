package com.test.T20180816

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class test0816_7 {

}

object test0816_7 {

  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 读取数据集
    val dataFrame = sparkSession.read.json("./files/test_7.json")
    dataFrame.printSchema()
    // 展示数据表信息
    // dataFrame.show()
    // 把dataFrame注册成为global temporary 视图
    dataFrame.createGlobalTempView("people")
    // 执行sql查询
    sparkSession.sql("select * from global_temp.people where age > 20").show()
    // 停止SparkContext
    sparkSession.stop()
  }
}
