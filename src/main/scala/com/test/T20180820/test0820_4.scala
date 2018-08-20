package com.test.T20180820

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

class test0820_4 {

}

object test0820_4 {
  /*
    * 基础方法
    * 连接zk,创建znode,更新znode
    */

  val TIME_OUT = 10000
  var zooKeeper: ZooKeeper = _

  def watcher = new Watcher() {
    def process(event: WatchedEvent) {
       println("已经触发了" + event.getType() + "事件！");
    }
  }
  def connect() {
    println("zk connect")
    zooKeeper = new ZooKeeper("bigdata04:2181,bigdata05:2181,bigdata06:2181,bigdata07:2181,bigdata08:2181", TIME_OUT, watcher )
  }

  def znodeCreate(znode: String, data: String) {
    connect()
    println(s"zk create /$znode , $data")
    zooKeeper.create(s"/$znode", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  def znodeDataSet(znode: String, data: String) {
    connect()
    println(s"zk data set /$znode")
    zooKeeper.setData(s"/$znode", data.getBytes(), -1)
  }

  /*
    * 获得znode数据
    * 判断znode是否存在
    * 更新znode数据
    */
  def znodeDataGet(znode: String): Array[String] = {
    connect()
    println(s"zk data get /$znode")
    try {
      new String(zooKeeper.getData(s"/$znode", true, null), "utf-8").split(",")
    } catch {
      case _: Exception => Array()
    }
  }

  def znodeIsExists(znode: String): Boolean = {
    connect()
    println(s"zk znode is exists /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => false
      case _ => true
    }
  }

  def offsetWork(znode: String, data: String) {
    connect()
    println(s"offset work /$znode")
    zooKeeper.exists(s"/$znode", false) match {
      case null => znodeCreate(znode, data)
      case _ => znodeDataSet(znode, data)
    }
    println("zk close !")
    zooKeeper.close()
  }

  def main(args: Array[String]): Unit = {
    //zk的路径
    val zkOffsetPath = "test0820Offset"
    znodeDataSet(zkOffsetPath,"2222")
  }
}