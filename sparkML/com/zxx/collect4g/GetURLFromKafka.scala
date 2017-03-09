package com.zxx.collect4g

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming._ 
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.api.java.function._
import org.apache.spark.streaming._
import org.apache.spark.streaming.api._

//spark-shell --master yarn --conf "spark.dynamicAllocation.enabled=false"
object GetURLFromKafka {
  //匹配微信公众号的正则表达式
  val pattern = """http://mp.weixin.qq.com""".r
  //url结果存储路径
  val path="/user/spark/weixin"
  val kafka="172.16.5.117:2181,172.16.5.118:2181,172.16.5.119:2181/kaflume"
  val g="newg"
  val t=Map("4g" -> 3)
  
  
  def main(args: Array[String]): Unit = {
    
    if(args.length < 4){
      System.err.println("Usage:GetURLFromKafka <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    
    val Array(zkQuorum,group,topics,numThreads) = args
    val sc=new SparkConf().setAppName("GetURLFromKafka")
    //val ssc=new StreamingContext(sparkConf,Seconds(30))
    val ssc=new StreamingContext(sc,Seconds(60))
    val topicMap=topics.split(",").map((_,numThreads.toInt )).toMap
   // val lines=KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_ONLY).map(_._2)
     val lines=KafkaUtils.createStream(ssc, kafka, g, t, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    //3 MSISDN
    //19 StartTime
    //63 DestinationURL
    //x => (pattern.findFirstIn(x._2),x)
    //找出微信公众号的连接，并匹配指定的手机号
    //var url=lines.map(_.split("\\|")).filter(_.length == 90).map{x => ("""http://mp.weixin.qq.com""".r.findFirstIn(x(62)),x(2),x(62))}.filter{x => (!x._1.isEmpty && Set("13379868631","18907553028").contains(x._2))}.map(x => (x._2,x._3))
     var url=lines.map(_.split("\\|")).filter(_.length == 90).map{x => ("""http://mp.weixin.qq.com""".r.findFirstIn(x(62)),x(2),x(62),x(18))}.filter{x => (!x._1.isEmpty )}.map(x => (x._2,x._3,x._4))
     url.filter(x => "13379868631" == x._1 || "18907553028" == x._1).print()
   // url.saveAsTextFiles(path, "txt")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)
    
  }
  
}