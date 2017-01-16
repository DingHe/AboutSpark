package com.zzx.sample

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.matching.Regex
import scala.io.Source
import scala.sys.process.ProcessBuilder.Source
import java.util.UUID
import java.io.PrintWriter
import java.io.File

object getURLContent {
  //var pattern = """http://(\w+\.)*(weixin\.|qq\.)(\w+\.)*\w+\W""".r
  var pattern = """http://mp.weixin.qq.com""".r
  var allCount=0
  var failCount=0

  def main(args: Array[String]): Unit = {
     //spark-shell --master yarn --name getURLContent --driver-memory 10g --executor-memory 2g  --executor-cores 2  --num-executors 20
//    val path="/user/hive/warehouse/hnlyw.db/t_4gphone_webbrowser/*/"
//    val sc=new SparkContext(new SparkConf().setMaster("local[4]").setAppName("getURLContent"))
//    val data=sc.textFile(path)
//    val fields=data.map{ _.split("\\|")}.filter { _.length == 90 }
//    val DestinationURL=fields.map { _(62)}
//   // val DomainName=DestinationURL.map { x => pattern.findFirstIn(x) }.filter{! _.isEmpty}.map {x => x match{ case Some(domain) => domain;case None => ""}}.distinct.repartition(1)
//     val DomainName=DestinationURL.map { x => (pattern.findFirstIn(x),x) }.filter{! _._1.isEmpty}.map{case Tuple2(a,b) => b}.repartition(10)
//   
//    DomainName.saveAsTextFile("/user/spark/4gresult")

      runGetWeixinNames()
    
  }
  
  
  
  def runGetURLContent()={
      val file=new File("G:\\weixin\\4gresult")
      val path=file.getAbsolutePath()
      file.list().foreach { x => println(path+"""\"""+x)
        getURLContent(path+"""\"""+x,"""G:\weixin\out""")
        allCount += 1
      }
  }
  
  
  def getURLContent(urlFileName:String,ContFileName:String):Unit={
    Source.fromFile(urlFileName).getLines().foreach 
    { x => val uuid=UUID.randomUUID().toString()
           val file=new File(ContFileName+"\\"+uuid+".html")
           val out=new PrintWriter(file)
           try{
             Source.fromURL(x,"utf-8").getLines().foreach { x => out.write(x)   }
          }catch {
            case t: Throwable =>  file.delete();
            failCount += 1
          }finally {
            out.close()
          }    
     }
  }
  
  def runGetWeixinNames()={
     val file=new File("""G:\weixin\out""")
     val path=file.getAbsolutePath()
     val outputfile=new File("""G:\weixin\weixin2.txt""")
     val outwriter=new PrintWriter(outputfile)
     println("begin")
     file.list().foreach {x =>
        val file=path+"""\"""+x
        val content=Source.fromFile(file, "utf-8").mkString
        val result=getWeixinNames(content)
        if (result.length() >= 1) outwriter.write(result+"\n")
        println(result)
        println("running...")
     }
     println("end")
     outwriter.close()
  }
  
  
  def getWeixinNames(content:String):String={
    val pattern="""id="post-user">(\S*)</a>""".r
    val result=pattern.findFirstIn(content)
    result.getOrElse("").replaceAll("""id="post-user">""", "").replace("""</a>""", "")
  }
    
  
}