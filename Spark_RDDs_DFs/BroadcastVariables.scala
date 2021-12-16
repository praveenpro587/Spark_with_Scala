package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source

object BroadcastVariables extends App{
  
  def loadboringWords():Set[String]={
    
    var boringwords:Set[String]=Set()
    val lines=Source.fromFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week2/boringwords.txt").getLines()
    
    for(line<-lines){
      boringwords+=line
    }
    boringwords
  }
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","BraodCastVar")
  var nameset=sc.broadcast(loadboringWords)
  
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week2/bigdatacampaigndata.csv")
  val maprdd=filerdd.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))
  val flatrdd=maprdd.flatMapValues(x=>x.split(" "))
  val words=flatrdd.map(x=>(x._2.toLowerCase(),x._1))
  val filterrdd=words.filter(x=>(!nameset.value(x._1)))
  val finalrdd=filterrdd.reduceByKey(_+_)
  val res=finalrdd.sortBy(x=>x._2,false)
  res.take(20).foreach(println)
}
