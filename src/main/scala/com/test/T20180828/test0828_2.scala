package com.test.T20180828

import org.apache.kafka.common.TopicPartition

import scala.io.Source

class test0828_2 {

}

object test0828_2 extends App {
  /**
    *
    */
  val localfile = Source.fromFile("./files/offset.log", "UTF-8")
  val fromOffsets = collection.mutable.Map[TopicPartition, Long]()
  for (line <- localfile.getLines()) {
    val array = line.split(",")
    fromOffsets += (new TopicPartition(array(0), array(1).toInt) -> array(2).toLong)
  }
  println(fromOffsets)
}