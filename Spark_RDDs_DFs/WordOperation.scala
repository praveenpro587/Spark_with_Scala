package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext



object WordOperation extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","WordOps")
  
  val rdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark/sample.txt")
  val flat=rdd.flatMap(x=>x.toLowerCase().split(" "))
  //val flat=rdd.flatMap(x=>x.split(" "))
  val mapwords=flat.map(x=>(x,1))
  val WC=mapwords.reduceByKey((x,y)=>x+y)
  //val top10=WC.sortBy(x=>x._2,false).take(10)
  val interchange=WC.map(x=>(x._2,x._1))
  val top10=interchange.sortByKey(false).take(10)
  val normal=top10.map(x=>(x._2,x._1))
  //normal.foreach(println)
  for(normal<- normal){
    
    val word=normal._1
    val count=normal._2
    println(s"$word=>$count")
    
  }
}
