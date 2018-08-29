package com.OSSDataSynchronization


import java.io.{File, FileWriter}
import java.sql.Timestamp
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, Properties}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.client.KuduClient
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext.get
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


class ConsumerMain {

}

object ConsumerMain extends App {

  val zk = ZookeeperManager
  val kf = KafkaManager
  val kd = KuduManager
  val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val ssc = new StreamingContext(conf, spark.streaming.Seconds(5))
  // spark参数信息(可在spark-submit执行命令行添加)
  conf.set("spark.streaming.stopGracefullyOnShutdown", "true") //优雅的关闭
  conf.set("spark.streaming.backpressure.enabled", "true") //激活削峰功能
  conf.set("spark.streaming.backpressure.initialRate", "5000") //第一次读取的最大数据值
  conf.set("spark.streaming.kafka.maxRatePerPartition", "2000") //每个进程每秒最多从kafka读取的数据条数
  // 消费主题
  val properties = new Properties()
  properties.load(this.getClass.getResourceAsStream("/config.properties"))
  val kuduMaster = properties.getProperty("kudu.master")
  val topics = properties.getProperty("kafka.topic").split(",").toSet
  val group = "test"
  // 消费者配置
  val kafkaParams = Map(
    // 用于初始化链接到集群的地址
    "bootstrap.servers" -> "bigdata03:9092,bigdata06:9092,bigdata08:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    // 用于标识这个消费者属于哪个消费团体
    "group.id" -> group,
    // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    // 从最新的开始消费
    "auto.offset.reset" -> "latest",
    // 如果是true，则这个消费者的偏移量会在后台自动提交
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  // 参数
  // 读取offset
  // 采用zk 本地文件存储方式读取 -- kafka自身保存的不需要处理
  // 创建数据流
  val stream = kf.createDirectStream(ssc, kafkaParams, topics)
  if (args.length == 1) {
    println("[ ConsumerMain ] exists parameters: maybe zk or localfile")
    args(0) match {
      case "zk" =>
        println(s"[ ConsumerMain ] parameter is correct as ${args(0)}: zookeeper storage offsets")
        val stream = kf.createDirectStreamReadOffset(ssc, kafkaParams, topics, args(0))
      case "local" =>
        println(s"[ ConsumerMain ] parameter is correct as ${args(0)}: local file storage offsets")
        val stream = kf.createDirectStreamReadOffset(ssc, kafkaParams, topics, args(0))
      case _ =>
        println("[ ConsumerMain ] parameter error")
        val stream = kf.createDirectStream(ssc, kafkaParams, topics)
    }
  }
  else {
    println("[ ConsumerMain ] process not exists parameters: kafka own storage")
    val stream = kf.createDirectStream(ssc, kafkaParams, topics)
  }

  // PROPERTIES文件读取
  val kuduClient = new KuduClient.KuduClientBuilder(properties.getProperty("kudu.master")).build()

  val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val dayFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val total = ssc.sparkContext.longAccumulator("total")
  val btName = new ArrayBuffer[String]()
  val currentTs = new ArrayBuffer[String]()
  // 保存偏移量
  stream.foreachRDD {
    (rdd, time) =>
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val bt_starTime = timeFormat.format(new Date())
        //  处理数据
        rdd.foreachPartition {
          lines => {
            lines.foreach(line => {
              total.add(1)
              // kf.dataParseJson(kuduClient, line.value())
              val tup = kd.kuduConnect(line.value())
              println(s"[ ConsumerMain ] table and current_timestamp: $tup")
              btName += tup._1
              currentTs += tup._2
            })
          }
        }
        val rddTime = new Timestamp(time.milliseconds).toString.split("\\.")(0)
        val bt_endTime = timeFormat.format(new Date())
        println(s"[ ConsumerMain ] ${btName} , ${currentTs}")
        println(s"[ ConsumerMain ] synchronous base table name traversal: ${btName.toList.distinct.mkString(",")}")
        println(s"[ ConsumerMain ] current_ts data time: ${currentTs.toList.distinct.mkString(",")}")

        //val logfile = "/tmp/topics/bt_log" + dayFormat.format(new Date()) + ".log"
        val logfile = "./files/bt_log" + dayFormat.format(new Date()) + ".log"
        //        val configuration = new Configuration()
        //        val configPath = new Path(logfile)
        //        val fileSystem: FileSystem = configPath.getFileSystem(configuration)
        //        var append: FSDataOutputStream = null;
        //        if (!fileSystem.exists(configPath)) {
        //          append = fileSystem.create(configPath)
        //        } else {
        //          append = fileSystem.append(configPath)
        //        }
        //        val td: Double = (Timestamp.valueOf(bt_endTime).getTime - Timestamp.valueOf(bt_starTime).getTime) / (1000)
        //        var result: String = null
        //        if (td == 0) {
        //          result = total.value.toString
        //        } else {
        //          val df: DecimalFormat = new DecimalFormat("0.00")
        //          result = df.format((total.value / td))
        //        }
        //        append.write(("\n"
        //          + "基表同步启动时间: " + bt_starTime + "\n"
        //          + "基表同步结束时间: " + bt_endTime + "\n"
        //          + "基表增量名称遍历: " + btName.toList.distinct.mkString(",") + "\n"
        //          + "增量数据同步时间: " + rddTime + "\n"
        //          + "增量过程同步总数: " + total.value + "\n"
        //          + "基表同步数据效率: " + result + " rec/s" + "\n"
        //          //          + "基表写入失败总数: " + errorTotal.value + "\n"
        //          ).getBytes("UTF-8"))
        //        total.reset()

        // 写本地文件系统
        val fw = new FileWriter(new File(logfile), true)
        val td: Double = (Timestamp.valueOf(bt_endTime).getTime - Timestamp.valueOf(bt_starTime).getTime) / (1000)
        var result: String = null
        if (td == 0) {
          result = total.value.toString
        } else {
          val df: DecimalFormat = new DecimalFormat("0.00")
          result = df.format((total.value / td))
        }
        fw.write("\n"
          + "基表同步启动时间: " + bt_starTime + "\n"
          + "基表同步结束时间: " + bt_endTime + "\n"
          + "基表增量名称遍历: " + btName.toList.distinct.mkString(",") + "\n"
          + "增量数据同步时间: " + rddTime + "\n"
          + "增量过程同步总数: " + total.value + "\n"
          + "基表同步数据效率: " + result + " rec/s" + "\n"
        )
        fw.close()
        // 清空数组
        btName.clear()
        currentTs.clear()

        //保存偏移量
        // zk存储偏移量
        val o: OffsetRange = offsetRanges(get.partitionId)
        println(s"[ ConsumerMain ] topic: ${o.topic}; partition: ${o.partition}; fromoffset: ${o.fromOffset}; utiloffset: ${o.untilOffset}")
        // 写zookeeper
        zk.zkSaveOffset(s"${o.topic}offset", s"${o.partition}", s"${o.topic},${o.partition},${o.fromOffset},${o.untilOffset}")
        // 关闭
        zk.zooKeeper.close()

        val offset: ArrayBuffer[String] = new ArrayBuffer[String]()
        for (i <- 0 until offsetRanges.length) {
          offset += (offsetRanges(i).topic + "," + offsetRanges(i).partition + "," + offsetRanges(i).fromOffset + "," + offsetRanges(i).untilOffset)
        }
        // 写本地文件系统 -覆盖
        val fwl = new FileWriter(new File("./files/offset.log"), false)
        fwl.write(s"${offset} \n")
        fwl.close()
        // 新版本Kafka自身保存
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      } else {
        println(s"[ ConsumerMain ] no data during this time period (${Seconds(5)})")
        //于HDFS上记录日志
        val nullTime = timeFormat.format(new Date())
        val rddTime = new Timestamp(time.milliseconds).toString.split("\\.")(0)
        //val logfile = "/tmp/topics/bt_log" + dayFormat.format(new Date()) + ".log"
        val logfile = "./files/bt_log" + dayFormat.format(new Date()) + ".log"
        //        val configuration = new Configuration()
        //        val configPath = new Path(logfile)
        //        val fileSystem: FileSystem = configPath.getFileSystem(configuration)
        //        var append: FSDataOutputStream = null;
        //        if (!fileSystem.exists(configPath)) {
        //          append = fileSystem.create(configPath)
        //        } else {
        //          append = fileSystem.append(configPath)
        //        }
        //        append.write(("\n"
        //          + "基表同步启动时间: " + nullTime + "\n"
        //          + "基表同步结束时间: " + nullTime + "\n"
        //          + "基表增量名称遍历: NULL " + "\n"
        //          + "增量数据同步时间: " + rddTime + "\n"
        //          + "增量过程同步总数: 0" + "\n"
        //          + "基表同步数据效率: 0 rec/s" + "\n"
        //          + "基表写入失败总数: 0" + "\n"
        //          ).getBytes("UTF-8"))
        //        append.close()
        //        fileSystem.close()
        // 写本地文件系统
        val fw = new FileWriter(new File(logfile), true)
        fw.write("\n"
          + "基表同步启动时间: " + nullTime + "\n"
          + "基表同步结束时间: " + nullTime + "\n"
          + "基表增量名称遍历: NULL " + "\n"
          + "增量数据同步时间: " + rddTime + "\n"
          + "增量过程同步总数: 0" + "\n"
          + "基表同步数据效率: 0 rec/s" + "\n"
          + "基表写入失败总数: 0" + "\n"
        )
        fw.close()

        // 清空数组
        btName.clear()
        currentTs.clear()
      }
  }
  ssc.start()
  ssc.awaitTermination()
}
