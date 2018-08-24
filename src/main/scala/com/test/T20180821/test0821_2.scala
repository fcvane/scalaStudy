package com.test.T20180821

import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat


class test0821_2 {

}
object test0821_2 extends App{


  /*
   *@param Hbase操作 -读取数据
   */

  val conf = new SparkConf().setAppName("test").setMaster("local[2]")
  val sc = new SparkContext(conf)
  // hbase表名，详情见同目录下的hbase.sql
  val tablename = "test0818"
  // 通过zookeeper获取HBase连接
  val hbaseConf = HBaseConfiguration.create()
  // 设置zooKeeper集群地址，也可以通过将hbase-site.xml，但是建议在程序里这样设置
  hbaseConf.set("hbase.zookeeper.quorum", "bigdata04,bigdata05,bigdata06,bigdata07,bigdata08")
  // 设置zookeeper连接端口，默认2181
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  /*  hbaseConf.addResource(new Path("/etc/hadoop/conf", "core-site.xml"))
    hbaseConf.addResource(new Path("/etc/hadoop/conf", "hdfs-site.xml"))
    hbaseConf.addResource(new Path("/etc/hbase/conf", "hbase-site.xml"))*/
  // zookeeper中hbase的根目录 -日常工作中默认的根目录不一定是hbase，这将导致读取不到hbase的meta元数据信息
  hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
  // 设置读取表名
  hbaseConf.set(TableInputFormat.INPUT_TABLE, tablename)
  // 设置读取列族
  //hbaseConf.set(TableInputFormat.SCAN_COLUMNS, "info")
  // 读取数据并转化成rdd
  // 使用newAPIHadoopRDD读取HBase
  val table = new HTable(hbaseConf, tablename)
  val put = new Put(Bytes.toBytes("001"))
  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CRM_AUTO_PZ_TYPE"), Bytes.toBytes("100000025"))
  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CRM_AUTO_PZ_ELSE_TYPE"), Bytes.toBytes("222"))
  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SR_STATE"), Bytes.toBytes("1"))
 // 转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset&&saveAsNewAPIHadoopDataset
  table.put(put)
  table.close()
  val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])
  // 行数统计
  val count = hbaseRDD.count()
  println(count)
//  sc.stop()
}