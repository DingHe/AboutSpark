package com.zxx.collect4g

import java.io.File
import java.io.PrintWriter
import scala.io.Source
import com.zzx.hbase.HbaseIO
import org.apache.hadoop.hbase.util.Bytes
import com.zzx.tencent.TencentWenZhi

//export JAVA_HOME=/usr1/kafka/jdk1.7.0_80
//export CLASSPATH=$CLASSPATH:`hbase classpath`
//scala -classpath ./HbaseIO.jar:$CLASSPATH

object FromURLGetWeiXinName {
  
  def getURLContent(url:String):String={
      Source.fromURL(url,"utf-8").mkString("")
  }
  
  def getWeixinName(content:String):String={
     val pattern="""id="post-user">(\S*)</a>""".r
     val result=pattern.findFirstIn(content)
     result.getOrElse("").replaceAll("""id="post-user">""", "").replace("""</a>""", "")
  }
  
  def main(args: Array[String]): Unit = {
    
    val SecretId="AKIDVt670lqQL4au324Rq282kGa3LQJLGzW6"
    val SecretKey="lNn13tWzFJAqLWWJuJVYR8ct31nMjuRo"
    val table=new HbaseIO("hnlyw:useraccessinfo")
    val user_table=new HbaseIO("hnlyw:userinfo")
    val weixin_table=new HbaseIO("hnlyw:weixinurl")
    val scanner=table.doScanData("20170220").iterator()
    
    val wenzhi=new TencentWenZhi(weixin_table,SecretId,SecretKey)

    while(scanner.hasNext()){
      try{
        var result=scanner.next()
        var key=Bytes.toString(result.getRow)
        var url=Bytes.toString(result.getValue(Bytes.toBytes("url"),Bytes.toBytes("info")))
        var content=getURLContent(url)
        var weixinname=getWeixinName(content)
        
        if(!weixinname.equals("")){
          System.out.println("weixinname="+weixinname)
          //增加微信号的计数
          user_table.increColumnValue(key.substring(0, 32),"weixin", weixinname, 1L)
          //获取相关的分类
          wenzhi.getKeyWordAndClassify(url,key.substring(0, 32));
          //记录文章的内容的关键词
          wenzhi.putIntoHbase("content")
          //增加关键词的计数
          if (wenzhi.getKeywords() != null){
              val keywordsitr=wenzhi.getKeywords().iterator()
              while(keywordsitr.hasNext())
                user_table.increColumnValue(key.substring(0, 32),"keywords",keywordsitr.next(), 1L)
          }
           
          //增加分类的计数
          if(wenzhi.getClasssify() != null){
              val classtifysitr=wenzhi.getClasssify().iterator();
              while(classtifysitr.hasNext())
                user_table.increColumnValue(key.substring(0, 32),"classtify",classtifysitr.next(), 1L)
          }
          
        }else{
          System.out.println("weixinname=NULL")
        }
        //删除已经处理的记录
        table.doDeleteData(key, "url", "info")
      }catch{
        case e:Exception => print(e.getStackTraceString)
      }
   }
    table.close()
    user_table.close()
    weixin_table.close()
  }
  
}