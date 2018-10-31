package com.test.T20181023

/**
  * Auther fcvane
  * Date 2018/10/24
  */
class test20181023_2 extends Runnable {
  override def run(): Unit = {
    println("Thread is Running")
  }

}

object test20181023_2 extends App {
  var e = new test20181023_2()
  var t = new Thread(e)
  t.start()
}
