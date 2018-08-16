package com.test.T20180816

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class test0816_5{

}
case class Person(name: String, age: Int)
object test0816_5 {
  /*
   *@param DataFrame操作
   * 类似关系数据库的表
   */
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.INFO)

    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    /*
     *在spark的早期版本中，SparkContext是spark的主要切入点，由于RDD是主要的API，通过sparkcontext来创建和操作RDD。
     * 对于每个其他的API，我们需要使用不同的context。
     * 例如，
     * 对于Streming，我们需要使用StreamingContext；
     * 对于sql，使用sqlContext；对于Hive，使用hiveContext。
     * 随着DataSet和DataFrame的API逐渐成为标准的API，就需要为他们建立接入点。
     * 在spark2.0中，引入SparkSession作为DataSet和DataFrame API的切入点，SparkSession封装了SparkConf、SparkContext和SQLContext。
     * 为了向后兼容，SQLContext和HiveContext也被保存下来。
     */
    // sparksql入口
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val info = List(("mike", 24), ("joe", 34), ("jack", 55))
    val infoRDD = sc.parallelize(info)
    // 创建dataframe
    val people = infoRDD.map(r => Person(r._1, r._2)).toDF()
    people.show()
    // 注册临时表
    people.registerTempTable("people")
    // 执行sql查询
    val subDF = sqlContext.sql("select * from people where age > 30").show()
  }
}