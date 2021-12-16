package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source
object BroadcastMoviesDS extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","MoviesRating")
  
  val ratingsRdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/ratings.dat")
  
  // get the movie id and rating
  val splitrdd=ratingsRdd.map(x=>(x.split("::")(1),x.split("::")(2)))
  
  
  val moviesRDD=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/movies.dat")
  val bcast=sc.broadcast(moviesRDD.map(x=>(x.split("::")(0),x.split("::")(1))).collect().toMap)
  //val braodcast=sc.broadcast(moviesRDD)
  // get the o/p=>(100,(4.0,1.0)),(100,(5.0,1.0))
  val newMapped=splitrdd.mapValues(x=>(x.toFloat,1.0))
  //get the o/p=>(100,(9.0,2.0))
  
  val reducedRDD=newMapped.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  //Filter based on condition like atleast 1000 people should rated the movie
  
  val filterRDD=reducedRDD.filter(x=>x._2._2>1000)
  
  //Calcuate avg rating and filter rating GE than 4.5
  val ratingsProcessed=filterRDD.mapValues(x=>(x._1/x._2)).filter(x=>x._2>4.5)
  
  
  //ratingsProcessed.collect.foreach(println)
  
  val FinalRdd=ratingsProcessed.map(x=>(x._1,x._2,bcast.value(x._1)))
  
  FinalRdd.collect.foreach(println)
  
  
  
  
}
