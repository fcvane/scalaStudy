package com.test.T20180816

import java.io.{File, PrintWriter}
import org.apache.log4j.{Level, Logger}
import scala.util.Random
import scala.io.Source

class test0816_2 {

}
object test0816_2 {
  /*
   *@param 文件操作
   *文件读写
   */
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.INFO)
    // 文件读取
    val rfile = Source.fromFile("./files/test_2.txt")
    for(line <- rfile.getLines)
      println(line)
    rfile.close

    // 文件写入
    // 创建字符输出流类对象和已存在的文件相关联。文件不存在的话，并创建。append状态为true则追加 false则覆盖
    val wfile = new File("./files/test_3.txt")
    val writer = new PrintWriter(wfile)
    // 随机数
    val rand = new Random()
    for(i <- 1 to 10)
      writer.write( i + " " + rand.nextInt(47) + '\n')
    // 清空缓冲区并完成文件写入操作
    writer.flush()
    writer.close()
  }
}