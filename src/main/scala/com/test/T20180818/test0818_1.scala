package com.test.T20180818

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}

class test0818_1 {

}

object test0818_1 extends App {

  /*
   *@param Hbase操作 -读取数据
   */

  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  // hbase表名，详情见同目录下的hbase.sql
  val tablename = "test0818"
  // 通过zookeeper获取HBase连接
  val hbaseConf = HBaseConfiguration.create()
  // 设置zooKeeper集群地址，也可以通过将hbase-site.xml，但是建议在程序里这样设置
  hbaseConf.set("hbase.zookeeper.quorum", "bigdata04,bigdata05,bigdata06,bigdata07,bigdata08")
  // 设置zookeeper连接端口，默认2181
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  // zookeeper中hbase的根目录 -日常工作中默认的根目录不一定是hbase，这将导致读取不到hbase的meta元数据信息
  hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
  // 设置读取表名
  hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
  // 设置读取列族
  hbaseConf.set(TableInputFormat.SCAN_COLUMNS, "info")
  // 读取数据并转化成rdd
  // 使用newAPIHadoopRDD读取HBase
  val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])
  // 行数统计
  val count = hbaseRDD.count()
  println(count)
  // 读取RDD结果集，返回一个MapPartitionsRDDid
  val resRDD = hbaseRDD.map(tuple => tuple._2)
  println(resRDD.foreach(println(_)))
  // 打印读取数据内容 (1)
  // 通过列族和列名获取列
  // 数据读取写入都必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
  resRDD.map(r => (Bytes.toString(r.getRow),
    Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("id"))),
    Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))),
    Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
  )).take(10).foreach(println(_))

  // 打印读取数据内容 (2)
  resRDD.foreach { r => {
    // 获取行键
    val key = Bytes.toString(r.getRow)
    // 通过列族和列名获取列
    val typenames = Bytes.toString(r.getValue("info".getBytes, "id".getBytes))
    if (key != null && typenames != null) {
      println(key + ":" + typenames)
    }
  }
  }
  sc.stop()
}
