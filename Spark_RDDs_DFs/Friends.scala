package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object Friends extends App{
  
  def parseLine(Line:String)={
    val fields=Line.split("::")
    val age=fields(2).toInt
    val friendscount=fields(3).toInt
    (age,friendscount)
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","FriendsData")
  
  val fileread=sc.textFile("C:/Users/prave/OneDrive/Desktop/friendsdata.csv")
  //val splitrdd=fileread.map(x=>(x.split("::")(2),x.split("::")(3).toInt))
  val maprdd=fileread.map(parseLine)
  val drdd=maprdd.map(x=>(x._1,(x._2,1)))
  //val drdd=maprdd.mapValues(x=>(x,1))--> We can use this also to avoid complex calc
  val sumrdd=drdd.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val finalres=sumrdd.map(x=>(x._1,x._2._1/x._2._2))
  //val finalres=sumrdd.mapValues(x=>x._1/x._2)--> We can use this also to avoid complex calc
  val result=finalres.sortBy(x=>x._1)
  result.collect.foreach(println)
  
}
