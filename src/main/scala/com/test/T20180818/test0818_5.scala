//package com.test.T20180818
//
//import org.apache.kudu.client.KuduClient.KuduClientBuilder
//import org.apache.kudu.client.{KuduPredicate, KuduScanToken}
//import org.apache.spark.{SparkConf, SparkContext}
//
//class test0818_5 {
//
//}
//
//object test0818_5 extends App {
//  /*
//   *@param kudu操作  -kudu API
//   */
//  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
//  val sc = new SparkContext(conf)
//  // kudu master节点
//  val kuduMaster = "bigdata02:7051,bigdata03:7051,bigdata04:7051,bigdata05:7051,bigdata06:7051,bigdata07:7051,bigdata08:7051"
//  // 客户端
//  val client = new KuduClientBuilder(kuduMaster).build()
//  // 获取表信息
//  // 编译器会提示无法找到foreach方法。因为client.getTablesList.getTablesList的类型为java.util.List
//  // 若要将其转换为Scala的集合，就需要增加如下语句
//  // import scala.collection.JavaConversions._
//  //  client.getTablesList.getTablesList.foreach {
//  //    println
//  //  }
//  val table = client.openTable("test0818")
//  val schema = table.getSchema()
//  // 设置搜索条件
//  // newComparisonPredicate 在一个整数或时间戳列上创建一个新的比较谓词。
//  val kp = KuduPredicate.newComparisonPredicate(schema.getColumn("age"), KuduPredicate.ComparisonOp.GREATER_EQUAL, 20)
//  // 开始扫描
//  val scanner = client.newScanTokenBuilder(table).addPredicate(kp).build()
//  println(scanner.hashCode())
//  val token = scanner.get(0)
//  val scan = KuduScanToken.deserializeIntoScanner(token.serialize(), client)
//  println(scan)
//  println(scan.hasMoreRows())
//  // 输出行
//  while (scan.hasMoreRows()) {
//    val results = scan.nextRows()
//    while (results.hasNext()) {
//      val rowresult = results.next()
//      println(rowresult.getString("id"))
//    }
//  }
//  scan.close()
//  client.close()
//}