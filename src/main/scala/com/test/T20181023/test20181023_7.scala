package com.test.T20181023

/**
  * Auther fcvane
  * Date 2018/10/24
  */
class test20181023_7 extends Thread {
  override def run() {
    for (i <- 0 to 5) {
      println(i)
      Thread.sleep(500)
    }
  }

  def task() {
    for (i <- 0 to 5) {
      println(i)
      Thread.sleep(200)
    }
  }
}

object test20181023_7 extends App {
  var t1 = new test20181023_7()
  t1.start()
  t1.task()
}
