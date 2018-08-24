package com.OSSDataSynchronization

import java.util.Properties

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}


class ZookeeperManager {

}

object ZookeeperManager {
  /**
    * Zookeeper管理
    */

  val TIME_OUT = 5000
  var zooKeeper: ZooKeeper = _

  def watcher = new Watcher() {
    def process(event: WatchedEvent) {
      println("[ ZookeeperManager ] already triggered " + event.getType + " event !")
    }
  }

  // PROPERTIES文件读取
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))

  /**
    * 基础方法
    */
  def connect() {
    println(s"[ ZookeeperManager ] zk connect")
    zooKeeper = new ZooKeeper(properties.getProperty("zookeeper.quorm"), TIME_OUT, watcher)
  }

  /**
    * 创建znode
    *
    * @param znode 数据节点
    * @param data  节点数据
    */
  def znodeCreate(znode: String, data: String) {
    println(s"[ ZookeeperManager ] zk create /$znode , $data")
    zooKeeper.create(s"/$znode", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  /**
    * 更新znode
    *
    * @param znode 数据节点
    * @param data  节点数据
    */
  def znodeDataSet(znode: String, data: String) {
    println(s"[ ZookeeperManager ] zk data set /$znode")
    zooKeeper.setData(s"/$znode", data.getBytes(), -1)
  }

  /**
    * 获取znode数据
    *
    * @param znode 数据节点
    * @return Array[Array[String]] 返回二位数组
    **/
  def znodeDataGet(znode: String): Array[Array[String]] = {
    connect()
    println(s"[ ZookeeperManager ] zk data get /$znode")
    try {
      println("[ 11111111111111111111111111111111 ]")
      val parArray = zooKeeper.getChildren(s"/$znode", true).toArray
      if (parArray != null) {
        //        parArray.foreach(partition => {
        //          println(new String(zooKeeper.getData(s"/$znode/$partition", true, null), "utf-8").split(","))
        //        })
        println(parArray.map(x => new String(zooKeeper.getData(s"/$znode/$x", true, null), "utf-8").split(",")) + "1111")
        parArray.map(x => new String(zooKeeper.getData(s"/$znode/$x", true, null), "utf-8").split(",")).foreach(println(_))
        parArray.map(x => new String(zooKeeper.getData(s"/$znode/$x", true, null), "utf-8").split(","))
      }
      else {
        Array(new String(zooKeeper.getData(s"/$znode", true, null), "utf-8").split(","))
      }
    } catch {
      case _: Exception => {
        Array(Array())
      }
    }
  }

  /**
    * Kafka offset存储位置判断
    *
    * @param znode 数据节点
    */
  def znodeIsExists(znode: String): Boolean = {
    connect()
    zooKeeper.exists(s"/$znode", true) match {
      case null => false
      case _ => true
    }
  }

  /**
    * 保存每个批次的rdd的offset到zk中
    *
    * @param znode     数据节点
    * @param partition Kafka分区
    * @param  data     节点数据 格式: 主题1,分区序号1,变化前的偏移量1,变化后的偏移量1;主题2,分区序号2,变化前的偏移量2,变化后的偏移量2,......
    */
  def zkSaveOffset(znode: String, partition: String, data: String) {
    connect()
    println(s"[ ZookeeperManager ] offset work /$znode")
    zooKeeper.exists(s"/$znode/$partition", true) match {
      case null => {
        zooKeeper.exists(s"/$znode", true) match {
          case null => znodeCreate(znode, "offset")
          case _ => znodeCreate(s"$znode/$partition", data)
        }
        znodeCreate(s"$znode/$partition", data)
      }
      case _ => znodeDataSet(s"$znode/$partition", data)
    }
  }

  def main(args: Array[String]): Unit = {
    val znode = "test0820offset"
    val array = znodeDataGet(znode)
    array.foreach(arr => {
      println(s"[ ZookeeperManager ] topic: ${arr(0).toString}; partition: ${arr(1).toInt}; fromoffset: ${arr(2).toInt}; utiloffset: ${arr(2).toInt}")
    })
  }
}