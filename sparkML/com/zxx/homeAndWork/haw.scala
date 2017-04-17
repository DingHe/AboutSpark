package com.zxx.homeAndWork

import org.apache.spark.rdd.RDD
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
//spark-shell --master yarn --conf "spark.dynamicAllocation.enabled=true"  --conf "spark.yarn.queue=byy"
object haw {
   //判断是否是工作日
    def isWeekday(date : String) : Boolean = {
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val d : Date=sdf.parse(date)
      var cal:Calendar = Calendar.getInstance()
      cal.setTime(d)
      var week=cal.get(Calendar.DAY_OF_WEEK)
      week != Calendar.SATURDAY && week != Calendar.SUNDAY
    }
  //计算家的位置
    def GetUserHomeByFreq(SelectedUserLoc: RDD[String]): RDD[String] = {
    val Home =SelectedUserLoc.map(x => x.split(",") match { case Array(user, time, cell, lng,lat, province) => (user,time,cell,lng,lat)})
                .filter { x => x match{case (user, time, cell,lng,lat) => (time.substring(8, 10).toInt >= 20) || (time.substring(8,10).toInt <= 7)} }                         //time.substring(8, 10) hour
                  .map(x => x match { case (user, time, cell,lng,lat) => (user+"#"+cell+","+lng+","+lat , 1)}).reduceByKey(_ + _).map{ x =>
                       val user = x._1.split("#")(0)
                       val cell = x._1.split("#")(1)
                       val cell_count = x._2
                      (user, (cell, cell_count))
                   }.groupByKey().map { x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 > b._2); (x._1, sort_arr) }
                       .filter(x => x._2.length >= 1).map { x => val user = x._1; val cellWithCoord = x._2(0)._1;user + "," + cellWithCoord  }
         Home
  }
 //计算工作地的位置
  def GetUserWorkByFreq(SelectedUserLoc: RDD[String]): RDD[String] = {
  val Work = SelectedUserLoc.map(x => x.split(",")match { case Array(user, time, cell, lat, lont, province) => (user, time, cell, lat, lont)}).
                  filter(x => x match {case (user, time, cell, lat, lont) => isWeekday(time.substring(0, 8)) && ((time.substring(8, 10).toInt >= 10 && time.substring(8, 10).toInt <= 12) || (time.substring(8, 10).toInt >= 14 && time.substring(8, 10).toInt <= 17))}).
                    map(x => x match { case (user, time, cell, lat, lont) => (user+"#"+cell+","+lat+","+lont, 1)}).
                      reduceByKey(_ + _).map { x =>
                          val user = x._1.split("#")(0)
                          val cell = x._1.split("#")(1)
                          val cell_count = x._2
                         (user, (cell, cell_count))}.
                     groupByKey().map { x => val sort_arr = x._2.toArray.sortWith((a, b) => a._2 > b._2); (x._1, sort_arr) }.filter(x => x._2.length >= 1).map { x => val user = x._1; val workWithCoord = x._2(0)._1;user + "," + workWithCoord}
    Work
  }
  
  def main(args: Array[String]): Unit = {
    val hivesql="select a.mdn,a.datestr,a.ci,b.longitude,b.latitude,a.province from hnlyw.t_phone_attribution a join hnlyw.t_ci_lng_lat_distance b on a.ci=b.ci  where a.dt >='20170201' and a.dt <='20170301'"
    //val sc=new SparkContext(new SparkConf().setMaster("local[4]").setAppName("haw"))
    val sqlContext=new HiveContext(sc)
    val data = sqlContext.sql(hivesql)
    val rdd=data.rdd
    rdd.cache()
    val rddstr=rdd.map(_.mkString(","))
    val home=GetUserHomeByFreq(rddstr).repartition(5)
    val work=GetUserWorkByFreq(rddstr).repartition(5)
    home.saveAsTextFile("/user/spark/home1")
    work.saveAsTextFile("/user/spark/work1")
    
    
    
  }
}