package com.zzx.sample

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Check4GData {
  
  
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
    
  }
}