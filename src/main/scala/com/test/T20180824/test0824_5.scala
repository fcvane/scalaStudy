package com.test.T20180824

import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class test0824_5 {

}

case class by(sr_id: String)

case class hive_zw_detail_result_kb(result_id: String, judge_dept: String, big: String, small: String, cause: String, total: String)

object test0824_5 extends App {

  /*
   *@param Hbase操作 -读取数据
   */

  val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  // hbase表名，详情见同目录下的hbase.sql
  //val tablename = "db_zwfx:wt_crm_common"'
  import spark.implicits._
  import spark.sql

  println("hive开始查询not")
  val not = sql("select sr_id  from (select \"100\" sr_id from test_db.dual union all select \"200\" sr_id from test_db.dual) t").as[by]
  not.show()
  // 通过zookeeper获取HBase连接
  println(not.show())
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
    x.foreach { x => {
      println("foreach循环插入")
      var table: Table = null
      val put = new Put(Bytes.toBytes(x.sr_id.reverse))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CRM_AUTO_PZ_TYPE"), Bytes.toBytes("10000005"))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("CRM_AUTO_PZ_ELSE_TYPE"), Bytes.toBytes("无法归类"))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("SR_STATE"), Bytes.toBytes("1"))
      table.put(put)
      println("插入not完成")

      var pz: Table = null
      pz = connection.getTable(TableName.valueOf("test0827"))
      val get = new Get(Bytes.toBytes(x.sr_id.reverse))
      val put1 = new Put(Bytes.toBytes(x.sr_id.reverse))
      println(get + "1111111111111111111")
      if (!pz.exists(get)) {
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("flag_000999"), Bytes.toBytes("无法归类"))
        pz.put(put1)
      }
      table.close()
      pz.close()
    }
    }
  }
  }
  spark.stop()

}
