package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object Week12Assgn2MovRating extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  case class Rating(User_Id:Int,Movie_id:Int,Rating:Int,timestamp:String)
  val RatingRdd=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/ratings.dat")
  val RatingmapRdd=RatingRdd.map(x=>x.split("::")).map(x=>Rating(x(0).toInt,x(1).toInt,x(2).toInt,x(3)))
  
  import spark.implicits._
  val ratingDf=RatingmapRdd.toDF()
  
 //ratingDf.show()
  
  case class Movies(Movie_id:Int,Movie_Name:String,Movie_type:String)
  val MoviesRdd=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week3/movies.dat")
  
  val movieMapRdd=MoviesRdd.map(x=>x.split("::")).map(x=>Movies(x(0).toInt,x(1),x(2)))
  
  val movieDf=movieMapRdd.toDF().select("Movie_id","Movie_Name")
  
  val transformedMovDF=ratingDf.groupBy("Movie_id").agg(count("Rating").as("MovieViewCount"),avg("Rating").as("avgMovieRating"))
  .orderBy(desc("MovieViewCount"))
  
  //transformedMovDF.show()
  
  //Popular movies MovieViewCount>1000 and avgMovieRating>4.5
  
  val PopularMoviesDf=transformedMovDF.filter("MovieViewCount > 1000 and avgMovieRating >4.5")
  
  spark.sql("SET spark.sql.autoBroadcastJoinThreshold =-1")
  
  val Joincon=PopularMoviesDf.col("Movie_id")===movieDf.col("Movie_id")
  
  val finalPopularMvDf=PopularMoviesDf.join(broadcast(movieDf),Joincon,"inner")
  .drop(PopularMoviesDf.col("Movie_id"))
  .sort(desc("avgMovieRating"))
  
  finalPopularMvDf.drop("MovieViewCount","avgMovieRating","Movie_id").show(false)
  
  
}
