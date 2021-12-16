package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object MovieData extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","MoviesData")
  
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark/moviedata.data")
  //filerdd.foreach(println)
  val splitrdd=filerdd.map(x=>x.split("\t")(2).toInt)
  val res=splitrdd.countByValue()
  /*val maprdd=splitrdd.map(x=>(x,1))
  val reducerdd=maprdd.reduceByKey(_+_)
  val res=reducerdd.sortByKey().collect()*/
  res.foreach(println)
  for(res<-res){
    val rating=res._1
    val count=res._2
    println(s"No of $rating star movies are $count")
  }
}
