package com.test.T20181023

/**
  * Auther fcvane
  * Date 2018/10/24
  */
class test20181023_6 extends Thread {
  override def run() {
    for (i <- 0 to 5) {
      println(this.getName() + "----")
      println(this.getPriority() + "++++")
      Thread.sleep(500)
    }
  }
}

object test20181023_6 extends App {
  var t1 = new test20181023_6()
  var t2 = new test20181023_6()
  t1.setName("First Thread")
  t2.setName("Second Thread")
  t1.setPriority(Thread.MIN_PRIORITY)
  t2.setPriority(Thread.MAX_PRIORITY)
  t1.start()
  t2.start()
}