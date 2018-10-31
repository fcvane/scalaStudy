package com.test.T20181023

/**
  * Auther fcvane
  * Date 2018/10/24
  */
class test20181023_5 extends Thread {
  override def run() {
    for (i <- 0 to 5) {
      println(this.getName() + " - " + i)
      Thread.sleep(500)
    }
  }
}

object test20181023_5 extends App {
  var t1 = new test20181023_5()
  var t2 = new test20181023_5()
  var t3 = new test20181023_5()
  t1.setName("First Thread")
  t2.setName("Second Thread")
  t1.start()
  t2.start()
}
