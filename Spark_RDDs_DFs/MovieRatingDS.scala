package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object MovieRatingDS extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","MoviesRating")
  
  val ratingsRdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/ratings.dat")
  
  // get the movie id and rating
  val splitrdd=ratingsRdd.map(x=>(x.split("::")(1),x.split("::")(2)))
  
  
  val moviesRDD=sc.textFile("C:/Users/prave/OneDrive/Desktop/movies.dat")
  // get the movie id and movie name
  val splitmoviesrdd=moviesRDD.map(x=>(x.split("::")(0),x.split("::")(1)))
  
  
  // get the o/p=>(100,(4.0,1.0)),(100,(5.0,1.0))
  val newMapped=splitrdd.mapValues(x=>(x.toFloat,1.0))
  //get the o/p=>(100,(9.0,2.0))
  
  val reducedRDD=newMapped.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  //Filter based on condition like atleast 1000 people should rated the movie
  
  val filterRDD=reducedRDD.filter(x=>x._2._2>1000)
  
  //Calcuate avg rating and filter rating GE than 4.5
  val ratingsProcessed=filterRDD.mapValues(x=>(x._1/x._2)).filter(x=>x._2>4.5)
  
  
  
  //Joining RatingRDD and MoviesRDD
  val JoinedRDD=splitmoviesrdd.join(ratingsProcessed)
  
  //Getting only movie names and respective rating watched more than 1000
  val finalRDD=JoinedRDD.map(x=>(x._2._1,x._2._2))
  
  finalRDD.collect.foreach(println)
  
  
}
