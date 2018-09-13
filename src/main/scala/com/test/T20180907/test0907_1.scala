package com.test.T20180907

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Auther fcvane
  * Date 2018/9/7
  */
class test0907_1 {

}

object test0907_1 extends App{
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf)
  val hiveContext =new HiveContext(sc)

}