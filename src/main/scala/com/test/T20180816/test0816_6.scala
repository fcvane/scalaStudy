package com.test.T20180816

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class test0816_6 {

}
object test0816_6{
  /*
   *@param sparkSession实现dataframe
   */
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.INFO)
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 创建dataframe
    val people = sparkSession.createDataFrame(List(("Michael", 29), ("Andy", 30), ("Justin", 19)))
      .withColumnRenamed("_1", "name")
      .withColumnRenamed("_2", "age")
    // 展示数据表信息
    people.show()
    people.printSchema()
    // 过来age>30的信息
    people.filter(people.col("age") > 30).show()
    // 把dataFrame注册成为global temporary 视图
    people.createGlobalTempView("people")
    val subDF = sparkSession.sql("select * from global_temp.people where age > 20").show()
    // 停止SparkContext
    sparkSession.stop()
  }
}
