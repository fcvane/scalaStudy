package com.test.T20180815

class test0815_1 {

}

object test0815_1 {

  def hello(name: String = "World"): String = {
    "Hello " + name
  }

  def helloScala(): Unit = {
    println("Hello , Scala!")
  }

  def add(x: Int, y: Int): Int = {
    x + y
  }

  val add2 = (x: Int, y: Int) => x + y

  def add3(x: Int)(y: Int): Int = x + y

  def printEbentChild(c: String*): Unit = {
    c.foreach(x => println(x))
  }

  def main(args: Array[String]): Unit = {
    //    println("Hello , Scala!")
    //    println(hello("scala"))
    //    println(hello())
    //    println(hello)
    //    helloScala()
    //    helloScala
    //    add(1, 2)
    //    println(add2(1, 2))
    //    println(add3(1)(2))
    //    printEbentChild("a", "b", "c", "d")

    //    val x = 1
    //    val a = if (x > 1) 1 else 0
    //    println(a)

    //    var (n, r) = (10, 0)
    //    while (n > 0) {
    //      r = r + n
    //      n = n - 1
    //    }
    //    println(r)

    //    for (i <- 1 to 10) {
    //      println(i)
    //    }
    //    for (i <- 1 until 10) {
    //      println(i)
    //    }

    //    for (i <- 1 to 10 if i % 2 == 0) {
    //      println(i)
    //    }
      }
}