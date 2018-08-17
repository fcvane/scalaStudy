package com.test.T20180817

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

class test0817_3 {

}
object test0817_3 extends App {
  /*
   *@param hive操作
   */
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  // enableHiveSupport()，不然使用的是默认的配置，不会读取hive-site.xml
  // SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

  // spark操作hive需要加载hive-site.xml文件，文件存放位置可在$sSPARK_HOME/conf 或者 项目资源文件夹 resources
  hiveContext.sql("show databases").show()
  hiveContext.sql("select * from test_db.test").show()
}