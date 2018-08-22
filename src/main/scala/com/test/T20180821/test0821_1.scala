package com.test.T20180821

import java.util.Properties

class test0821_1 {

}

object test0821_1 extends App {

  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))
  println(properties.getProperty("zookeeper.quorm"))

}