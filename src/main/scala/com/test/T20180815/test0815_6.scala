package com.test.T20180815

/**
  * Auther fcvane
  * Date 2018/8/15
  */
class test0815_6 {

}
// 类型用val
//不用new就可以产生对象
case class Book(name: String, author: String)

object test0815_6 extends App {

  val macTalk = Book("MacTalk", "CJQ")
  macTalk match {
    case Book(name, author) => println("This is a book")
    case _ => println("unknown")
  }
}