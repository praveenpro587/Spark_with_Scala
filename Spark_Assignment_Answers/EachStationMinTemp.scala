//Working on temperature Dataset to find Min temperature Based on Station

package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.math._

object EachStationMinTemp extends App{
  
  def parseLine(Line:String)={
    val fields=Line.split(",")
    val stationID=fields(0)
    val entryType=fields(2)
    val temp=fields(3)
    (stationID,entryType,temp)
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","Problem2")
  
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/tempdata.csv")
  val mapdata=filerdd.map(parseLine)
  
  val mintemp=mapdata.filter(x=>x._2=="TMIN")
  val stationtemp=mintemp.map(x=>(x._1,x._3.toFloat))
  val finalres=stationtemp.reduceByKey((x,y)=>min(x,y))
  
  finalres.collect.foreach(println)
  
  for(result<-finalres){
    val station=result._1
    val temp=result._2
    val formattedTemp = f"$temp%.2f F"
    println(s"$station minimum temperature: $formattedTemp")
  }
  
  
}
