package com.zzx.sample

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.UnsupportedEncodingException
import java.net.URLEncoder
import java.net.URLDecoder

object Check4GData {
  val ENCODE = "utf-8";
  
  def main(args: Array[String]): Unit = {
    //spark-shell --master yarn --name Check4GData --driver-memory 10g --executor-memory 2g  --executor-cores 2  --num-executors 20
    val path="/user/hive/warehouse/hnlyw.db/t_4gphone_webbrowser/*/"
    val sc=new SparkContext(new SparkConf().setMaster("local[4]").setAppName("Check4GData"))
    
//    val data=sc.wholeTextFiles(path)
//    val filenames=data.map{case (filename,contect) => filename}
//    filenames.take(10).foreach { println}
    
    
   // val phone_rdd=sc.textFile(path)
   // val fields_rdd=phone_rdd.map(_.split('\001'))
    //校验字段数
    val data=sc.textFile(path)
    val fields_count=data.map { _.split("\\|").length}
    val value_count=fields_count.countByValue()
    val sort_count_byvalue=value_count.toList.sortBy(- _._2)
    sort_count_byvalue.mkString("\n")
    value_count.toList.map(_._2).reduce(_ + _)
    
    //校验http内容
    val lenAndcontext=data.map{context => (context.split("\\|").length,context)}
    val len63Conext=lenAndcontext.map{case(len,conext) => if(len == 63) conext}
    len63Conext.saveAsTextFile("/user/spark/4gresult")
    
    
    //检查URL信息
    val fields=data.map { _.split("\\|")}.filter { _.length == 90 }
    val ContentTypeAndURL=fields.map{x => (x(66),x(62))}
    val decoderURL=ContentTypeAndURL.filter(_._1.toLowerCase == "application/x-www-form-urlencoded").map{case (contentType,url) => getURLDecoderString(url)}
    val containHotel=decoderURL.filter { _.contains("宾馆") }
    decoderURL.saveAsTextFile("/user/spark/4gresult")
    
  }
  
  def getURLDecoderString(str:String):String={
    var result=""
    if(null == str) return result
    try{
      result = URLDecoder.decode(str.replaceAll("%", "%25"),ENCODE)
    }catch{
      case ex : UnsupportedEncodingException => {ex.printStackTrace()}
    }
    result
  }
  
  def getURLEncoderString(str:String):String={
    var result=""
    if(null == str) return result
    try{
      result=URLEncoder.encode(str,ENCODE )
    }catch{
      case e:UnsupportedEncodingException => {e.printStackTrace()}
    }
    result
  }
  
  
}