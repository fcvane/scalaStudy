package com.test.T20180818

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

class test0818_3 {

}

object test0818_3 extends App {

  /*
   *@param Hbase操作 -写入数据(2)
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
  hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
  // Spark使用saveAsNewAPIHadoopDataset写数据到hbase bug解决
  // IMPORTANT: must set the attribute to solve the problem (can't create path from null string )
  hbaseConf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
  // 写入数据 (2)
  // 使用saveAsNewAPIHadoopDataset写入数据
  // 注意和test0818_2比较
  // import org.apache.hadoop.hbase.mapred.TableOutputFormat && org.apache.hadoop.hbase.mapreduce.TableOutputFormat
  val job = new Job(hbaseConf)
  job.setOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setOutputValueClass(classOf[Result])
  job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
  // 创建RDD
  val indataRDD = sc.makeRDD(Array("007,7,G,25", "008,8,H,33", "009,9,I,24"))
  val dataRDD = indataRDD.map(_.split(',')).map { arr => {
    // 指定rowkey
    val put = new Put(Bytes.toBytes(arr(0)))
    // Put.add方法接收三个参数：列族,列名,数据
    put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(arr(1)))
    put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(2)))
    put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3)))
    (new ImmutableBytesWritable, put)
  }
  }
  dataRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
  sc.stop()
}
