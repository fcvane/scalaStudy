package com.test.T20180815

class test0815_4 {

}

class ApplyTest {
  def apply() = "Apply"

  def test: Unit = {
    println("test")
  }
}
//单例
object ApplyTest {
  var count = 0
  def apply() = new ApplyTest
  def static: Unit = {
    println("I'm a static method")
  }
  def inct: Unit = {
    count = count + 1
  }
}

object test0815_4 extends App {
  //  ApplyTest.static
  //  val a = ApplyTest() // 类名加括号调用ApplyTest对象的apply方法
  //  a.test
  //  val t = new ApplyTest
  //  println(t()) //对象加括号调用ApplyTest类的apply方法
  //  println(t)
  for (i <- 1 to 10) {
    ApplyTest.inct
  }
  println(ApplyTest.count)
}
