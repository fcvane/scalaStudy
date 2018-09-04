package com.test.T20180815

/**
  * Auther fcvane
  * Date 2018/8/15
  */
class test0815_5 {

}

object test0815_5 extends App {
  val value = 2
  val result = value match {
    case 1 => "one"
    case 2 => "two"
    case _ => "other"
  }

  val result2 = value match {
    case i if i == 1 => "one"
    case i if i == 2 => "two"
    case _ => "other"
  }
  println("result of match is :" + result)
  println("result of match is :" + result2)

  def t(obj: Any) = obj match {
    case x: Int => println("Int")
    case x: String => println("String")
    case _ => println("unknown type")
  }

  t(1)
}
