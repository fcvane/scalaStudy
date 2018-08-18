package com.test.T20180818

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

class test0818_2 {

}

object test0818_2 extends App {

  /*
   *@param Hbase操作 -写入数据(1)
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
  // 设置写入表名
  // hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
  // 写入数据 (1)
  // 使用saveAsHadoopDataset写入数据
  val jobConf = new JobConf(hbaseConf)
  jobConf.setOutputFormat(classOf[TableOutputFormat])
  jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
  // 创建RDD
  val indataRDD = sc.parallelize(Array("004,4,D,25", "005,5,E,33", "006,6,F,24"))
  val dataRDD = indataRDD.map(_.split(',')).map { arr => {
    // 指定rowkey
    val put = new Put(Bytes.toBytes(arr(0)))
    // Put.add方法接收三个参数：列族,列名,数据
    put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(arr(1)))
    put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(2)))
    put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3)))
    // 转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset&&saveAsNewAPIHadoopDataset
    (new ImmutableBytesWritable, put)
  }
  }
  dataRDD.saveAsHadoopDataset(jobConf)

  sc.stop()

}
