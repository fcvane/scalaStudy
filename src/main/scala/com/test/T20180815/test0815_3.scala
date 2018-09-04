package com.test.T20180815

class test0815_3 {

}

abstract class Person1 {
  def speak

  var name: String
  var age: Int
}
class Student1 extends Person1 {
  def speak: Unit = {
    println("speak!")
  }
  var name = "AAA"
  var age = 18
}

//trait Logger {
//  def log(msg: String): Unit = {
//    println("msg: " + msg)
//  }
//}
//extends -> with
//class Test extends Logger {
//  def test: Unit = {
//    log("XXX")
//  }
//}
//trait有具体实现方法的接口
//trait Logger {
//  def log(msg: String)
//}
//trait ConsoleLogger extends Logger {
//   def log(msg: String): Unit = {
//    println(msg)
//  }
//}
//class Test extends ConsoleLogger{
//  def test: Unit ={
//    log("XXX")
//  }
//}

trait ConsoleLogger {
  def log(msg: String): Unit = {
    println(msg)
  }
}
trait MessageLogger extends ConsoleLogger {
  override def log(msg: String): Unit = {
    println(msg)
  }
}
abstract class Account {
  def save
}
class MyAccount extends Account with ConsoleLogger {
  def save: Unit = {
    log("100")
  }
}

object test0815_3 extends App {
  //  val s = new Student1
  //  s.speak
  //  println(s.name + " : " + s.age)

  //  val t = new Test
  //  t.test

  val acc = new MyAccount
  acc.save
  val bcc = new MyAccount with MessageLogger
  bcc.save

}
