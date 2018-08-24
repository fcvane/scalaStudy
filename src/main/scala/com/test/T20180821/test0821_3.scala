package com.test.T20180821

import org.apache.hadoop.hbase.client.{Put, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, _}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class test0821_3 {

}

case class by(sr_id: String)

object test0821_3 extends App {

  /*
   *@param Hbase操作 -读取数据
   */
  val sparkConf = new SparkConf().setAppName("test")//.setMaster("local[2]")
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

  import spark.implicits._
  import spark.sql

  println("hive开始查询")
  val not = sql("select sr_id  from (select \"100\" sr_id from test_db.dual union all select \"200\" sr_id from test_db.dual) t").as[by]
  // 通过zookeeper获取HBase连接
  println(not)
  not.foreachPartition { x => {
    println("foreachPartition循环")
    val hbaseConf = HBaseConfiguration.create()
    println("hbase表创建完成")
    // 设置zooKeeper集群地址，也可以通过将hbase-site.xml，但是建议在程序里这样设置
    hbaseConf.set("hbase.zookeeper.quorum", "bigdata04,bigdata05,bigdata06,bigdata07,bigdata08")
    // 设置zookeeper连接端口，默认2181
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase")
    // 使用newAPIHadoopRDD读取HBase
    println("创建hbase表")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    val table = connection.getTable(TableName.valueOf("test0818"))
    //    val table = new HTable(hbaseConf, "test0818")
    x.foreach { x => {
      println("foreach循环插入")
      val put = new Put(Bytes.toBytes(x.sr_id.reverse))
      println(x.sr_id.reverse, "---------------------")
      println("1111111111111111111111111111111111")
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CRM_AUTO_PZ_TYPE"), Bytes.toBytes("100000000"))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CRM_AUTO_PZ_ELSE_TYPE"), Bytes.toBytes("111"))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SR_STATE"), Bytes.toBytes("1"))
      table.put(put)
      println("2222222222222222222222222222222222")
      println("插入完成")
    }
    }
  }
  }
}