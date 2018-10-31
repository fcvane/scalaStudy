package com.test.T20181023

/**
  * Auther fcvane
  * Date 2018/10/24
  */
class test20181023_4 extends Thread {
  override def run() {
    for (i <- 0 to 5) {
      println(i)
      Thread.sleep(500)
    }
  }

}

object test20181023_4 extends App {
  var t1 = new test20181023_4()
  var t2 = new test20181023_4()
  var t3 = new test20181023_4()
  t1.start()
  t1.join()
  t2.start()
  t3.start()
}

