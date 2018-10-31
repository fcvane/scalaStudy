package com.hdfs.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Auther fcvane
  * Date 2018/10/16
  */
class HdfsClientConf(val coreStiteXMLPath: String, val hdfsStiteXMLPath: String) {
  private val conf = new Configuration()
  conf.addResource(new Path(coreStiteXMLPath))
  conf.addResource(new Path(hdfsStiteXMLPath))

  def getHdfsClientConf(): Configuration = conf

  def getHdpFileSystem(): FileSystem = FileSystem.get(conf);
}

object HdfsClientConf extends App {

  private var instance: HdfsClientConf = null

  def getOneTimeInstance(coreStiteXMLPath: String, hdfsStiteXMLPath: String): Configuration = {
    new HdfsClientConf(coreStiteXMLPath, hdfsStiteXMLPath).getHdfsClientConf()
  }

  def setSingltonInstance(coreStiteXMLPath: String, hdfsStiteXMLPath: String): Configuration = {
    if (instance == null)
      instance = new HdfsClientConf(coreStiteXMLPath, hdfsStiteXMLPath)

    instance.getHdfsClientConf()
  }

  def getSingletonInstance(): HdfsClientConf = {
    if (instance == null)
      throw new NullPointerException("Instanciate HdfsClientConf before retriving")

    instance
  }
}