package com.test.T20180815

class test0815_2 {

}

//class Persion {
//  //初始化 需要type类型
//  var name: String = _ // getter and setter
//  val age = 18 // getter
//  private[this] val gender = "male" //私有常量
//}

//主构造器直接跟在类名后面，主构造器的参数最后会被编译成字段
//主构造器执行的时候，会执行类中的所有语句
//假设参数声明时不带val或者var，那么相当于private[this]
class Persion(var name: String, val age: Int) {
  println("this is the primary constructor")
  var gender: String = _
  val school = "XXX"

  //附属构造器名称为this
  //每个附属构造器必须首先调用的子构造器或者父构造器
  def this(name: String, age: Int, gender: String) {
    //在任何一个子构造器第一行一定要调用现在已经存在的构造器
    this(name, age)
    this.gender = gender
  }

}

class Student(name: String, age: Int, val major: String) extends Persion(name, age) {
  println("this is the subclass of Person, major is: " + major)
  override val school: String = "AAA"
  override def toString: String = "Override toString"
}

object test0815_2 {
  def main(args: Array[String]): Unit = {
    //    val p = new Persion
    //    p.name = "Jacky"
    //    println(p.name + " : " + p.age)
    //    val p = new Persion("Jacky", 20, "male")
    //    println(p.name + " : " + p.gender)

    val s = new Student("Jestin", 20, "Math")
    println(s.toString)
  }
}
