package com.zxx.collect4g

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.zzx.hbase.HbaseIO

object FindWeiXinURL {
    //val pattern = """http://(\w+\.)*(weixin\.|qq\.)(\w+\.)*\w+\W""".r
  val pattern = """http://mp.weixin.qq.com""".r
  def main(args: Array[String]): Unit = {
         //spark-shell --master yarn --name FindWeiXinURL --driver-memory 10g  --jars ./HbaseIO.jar --executor-memory 2g  --executor-cores 2  --num-executors 20
    val path="/user/hive/warehouse/hnlyw.db/t_4gphone_webbrowser/20170221*/"
    val sc=new SparkContext(new SparkConf().setMaster("local[4]").setAppName("getURLContent"))
    
    val data=sc.textFile(path)
    val fields=data.map{ _.split("\\|")}.filter(_.length == 21)
    val DestinationURL=fields.map { x => (x(1)+"-"+x(11),x(16))}
    val DomainName=DestinationURL.map { x => (pattern.findFirstIn(x._2),x) }.filter{! _._1.isEmpty}.map{_._2}.repartition(10)
    DomainName.mapPartitions(x => {
          val table=new HbaseIO("hnlyw:useraccessinfo")
          while(x.hasNext){
            val pair=x.next()
            table.doPutData(pair._1, "url", "info", pair._2)
          }
          table.close()
          x
       }, true).count()
   
    //DomainName.saveAsTextFile("/user/spark/4gresult")
  }
}