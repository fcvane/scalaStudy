package com.test.T20181023

/**
  * Auther fcvane
  * Date 2018/10/24
  */
class test20181023_1 extends Thread {
  override def run(): Unit = {
    println("Thread is Running")
  }

}

object test20181023_1 extends App {
  var t = new test20181023_1()
  t.start()
}