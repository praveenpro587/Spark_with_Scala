package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source

object AddsDataProblem extends App{
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","AddsData")
  
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/bigdatacampaigndata.csv")
  val maprdd=filerdd.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))
  val flatrdd=maprdd.flatMapValues(x=>x.split(" "))
  val words=flatrdd.map(x=>(x._2,x._1))
  val finalrdd=words.reduceByKey(_+_)
  val res=finalrdd.sortBy(x=>x._2,false)
  res.take(20).foreach(println)
}
