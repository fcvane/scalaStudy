package com.test.T20180816

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

class test0816_3 {

}
object test0816_3 {
  /*
   *@param 均值
   * 数组和文本读取
   */
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.INFO)

    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    // 创建RDD
    val data = sc.parallelize(List(0,10,20,30,40))
    // 统计总数
    val count = data.count()
    // 总和
    val valueSum = data.filter(_ > 0).reduce(_ + _)
    // 计算
    val avg:Double = valueSum / count
    println (avg)

    // 从外部存储中读取数据来创建 RDD
    val file = "./files/test_3.txt"
    val textFile = sc.textFile(file)
    val count2 = textFile.count()
    // 空格做分隔符 基础数据类型转换成Int , 统计总和
    val valueSum2 = textFile.map(x => x.split(" ")(1)).map(x => Integer.parseInt(String.valueOf(x))).reduce(_ + _)
    val avg2:Double = valueSum2 / count2
    println(avg2)
  }
}