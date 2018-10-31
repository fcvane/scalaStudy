package com.test.T20180816

import java.text.SimpleDateFormat

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

class test0816_1 {

}

object test0816_1 {
  /*
   * @param 文本词频数统计
   * 实现 本地模式运行、文件加载、时间格式化、结果打印等
   */
  def main(args: Array[String]): Unit = {
    val path = new Path("hdfs:///user/s_it_res/synchronize/topics/wordCount_2018-09-18");
    // 设置日志级别
    Logger.getLogger("org").setLevel(Level.INFO)
    // 时间格式化
    val fileName: String = "wordCount_"
    val currentTime = System.currentTimeMillis
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    // 加载外部设置
    // 创建一个local StreamingContext，包含4个工作线程，并将批次间隔设为10秒
    // Master至少需要2个CPU核，以避免出现任务饿死的情况
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    // 初始化入参SparkConf ，程序主入口
    // 用于连接Spark集群、创建RDD、累加器（accumlator）、广播变量（broadcast variables）
    val sc = new SparkContext(conf)
    // 文件路径
    val file = "./files/test_1.txt"
    // 读取文件 2-分区数
    val textFile = sc.textFile(file, 2)
    // 将每一行分割成多个单词 单词进行计数 -map
    val words = textFile.flatMap(_.split(" ").map(x => (x, 1)))
    // 统计总数 -reduce
    val wordCounts = words.reduceByKey(_ + _)
    // 打印
    wordCounts.foreach(print(_))
    // 加载文件
    wordCounts.saveAsTextFile("./files/" + fileName + formatter.format(currentTime))
    // 停止SparkContext
    sc.stop()
  }
}