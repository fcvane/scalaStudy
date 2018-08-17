package com.test.T20180816

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

class test0816_4 {

}

object test0816_4 {
  /*
   *@param RDD基础转换
   */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)

    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    // 定义两个RDD
    val data1 = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    val data2 = sc.parallelize(List(3, 4, 5, 6, 7, 8))
    // union 合集
    val data_union = data1.union(data2)
    data_union.foreach(println(_, "union"))
    // intersection 交集
    val data_inter = data1.intersection(data2)
    data_inter.foreach(println(_, "intersection"))
    // subtract data1中出现但是data2没有出现的
    val data_sub = data1.subtract(data2)
    data_sub.foreach(println(_, "subtract"))
    // 停止SparkContext
    sc.stop()
  }
}