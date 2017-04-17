package com.zxx.collect4g

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import scala.runtime.StringFormat


//spark-shell --master yarn --conf spark.yarn.queue=byy --executor-memory 20g --executor-cores 8 --conf spark.yarn.executor.memoryOverhead=4096
class FindTop10 {
     
  val agentpattern = """;\s*(\w*[-]*\w*\s*\w*[-]*\w*)\s*(Build/)""".r.unanchored
  val urlpattern="""http://([\w.]+)/""".r.unanchored
  val agentpath="/user/spark/agent"
  val postfixdomain="""\.(\w+\.[a-zA-Z]+)$""".r.unanchored
  val sql="""select b.scenic_id,tmp.mdn,tmp.destinationurl,tmp.useragent,tmp.destinationip from 
           (select A.msisdn as mdn, A.starttime as datestr,substring(A.ecgi,length(A.ecgi)-8,9) as ci,destinationurl,useragent,destinationip from hnlyw.t_4gphone_webbrowser A   
           where A.begindt >= '20170330000' and A.begindt <= '2017040423' ) tmp join  hnlyw.t_scenic_ci b on tmp.ci=b.ci
           where b.scenic_id='d2e096b3f1a38499971d76e70ebec553' or b.scenic_id = '4bbac6e02e4d98ede8d13602fc9536b8' """
  
  
  val sqlyunnan="""select b.scenicname as scenic_id,tmp.mdn,tmp.destinationurl,tmp.DomainName,tmp.useragent,tmp.destinationip from 
           (select A.msisdn as mdn, A.starttime as datestr,eNB_ID as ci,destinationurl,useragent,destinationip,DomainName from qm.t_4Gphone_WebBrowser A ) tmp 
           join  (select distinct NodeBID,scenicname from qm.t_scenic_ci_yunnan) b on tmp.ci=b.NodeBID
           """



  def main(args: Array[String]): Unit = {
         //spark-shell --master yarn --name FindWeiXinURL --driver-memory 10g  --jars ./HbaseIO.jar --executor-memory 2g  --executor-cores 2  --num-executors 20

    val sc=new SparkContext(new SparkConf().setMaster("local[4]").setAppName("getURLContent"))
    val hivescontent=new HiveContext(sc)
    val dataframe=hivescontent.sql(sql);
//    val dataframe=hivescontent.sql(sqlyunnan);
   //终端类型
    val useragent=dataframe.select("scenic_id", "useragent").rdd.map{case Row(scenic_id:String,useragent:String) =>
      var agent= if (useragent.contains("iPhone")) "iPhone" else (useragent match { case agentpattern(phone,_*) => phone  case _ => ""})
      (scenic_id,agent.trim())
    }.filter(_._2 != "").map{x => (x._1+","+x._2,1)}
    useragent.reduceByKey(_ + _).sortBy(x => {var key=x._1.split(","); key(0)+(new StringFormat(x._2.toString())).formatted("%8s").replaceAll(" ", "0")}, false, 1).map(x => x._1+","+x._2).saveAsTextFile(agentpath)
 
    //域名
    val urldata=dataframe.select("scenic_id", "destinationurl").rdd.map{case Row(scenic_id:String,url:String) =>
      var urldomain= url match { case urlpattern(domain,_*) =>  domain  case _ => ""}  
      (scenic_id,urldomain)
    }.filter(_._2 != "").map{x => (x._1+","+x._2,1)}
    urldata.reduceByKey(_ + _).sortBy(x => {var key=x._1.split(","); key(0)+(new StringFormat(x._2.toString())).formatted("%8s").replaceAll(" ", "0")}, false, 1).saveAsTextFile("/user/spark/domain")
    
    
    //IP
    val ip=dataframe.select("scenic_id", "destinationip").rdd.map{case Row(scenic_id:String,destinationip:String) => 
          (scenic_id+","+destinationip,1)
    }
    ip.reduceByKey(_ + _).sortBy(x => {var key=x._1.split(","); key(0)+(new StringFormat(x._2.toString())).formatted("%8s").replaceAll(" ", "0")}, false, 1).saveAsTextFile("/user/spark/ip")
    
    //云南域名
    val domain=dataframe.select("scenic_id", "DomainName").rdd.filter(x => !x.isNullAt(1)).map{case Row(scenic_id:String,domain:String) => 
          val postfix=domain match { case postfixdomain(post,_*) => post case _ => ""}
          (scenic_id,postfix)
    }.filter(_._2 != "").map(x => (x._1+","+x._2,1))
    domain.reduceByKey(_ + _).filter(_._2 > 50).sortBy(x => {var key=x._1.split(","); key(0)+(new StringFormat(x._2.toString())).formatted("%8s").replaceAll(" ", "0")}, false, 1).saveAsTextFile("/user/spark/domain")
    //海南域名后缀
    val hnpostprix=dataframe.select("scenic_id", "destinationurl").rdd.map{case Row(scenic_id:String,url:String) =>
      var urldomain= url match { case urlpattern(domain,_*) =>  domain  case _ => ""}  
      (scenic_id,urldomain)
    }.map{x =>
      val postfix=x._2 match { case postfixdomain(post,_*) => post case _ => ""}
      (x._1,postfix)
    }.filter(_._2 != "").map{x => (x._1+","+x._2,1)}
    hnpostprix.reduceByKey(_ + _).sortBy(x => {var key=x._1.split(","); key(0)+(new StringFormat(x._2.toString())).formatted("%8s").replaceAll(" ", "0")}, false, 1).saveAsTextFile("/user/spark/domain")
    
    
    
    
    
    
  }
}