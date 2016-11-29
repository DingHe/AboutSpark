package com.xdh

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame


//EXCEL的java库 JExcelAPI

object testsparkSQL {
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[4]").setAppName("testsparkSQL")
    val sc=new SparkContext(conf)
    val hiveCtx=new HiveContext(sc)
    //val input=hiveCtx.jsonFile("/user/hdfs/testweet.json")
    //从1.4版本后DataFrameReader封装了所有读数据的接口
    val input=hiveCtx.read.json("/user/hdfs/testweet.json")
    input.registerTempTable("tweets")
    val topTweets=hiveCtx.sql("select text,retweetCount from tweets order by retweetCount limit 10")
    topTweets.first()
    topTweets.inputFiles
    topTweets.count()
    
    //缓存表
    hiveCtx.cacheTable("tweets")
    
  
  }
  def testHive():Unit = {
      val conf=new SparkConf().setMaster("local[4]").setAppName("testsparkSQL")
      val sc=new SparkContext(conf)
      val hiveCtx=new HiveContext(sc)
      val rows=hiveCtx.sql("select * from hnlyw.t_md5_phone")
      val keys=rows
      
      
      //主要测试HiveContext
      hiveCtx.getAllConfs.foreach(println)
      hiveCtx.tableNames("hnlyw").foreach { println }
  }
    
  
  
  
}