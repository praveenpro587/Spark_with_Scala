package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object Week12Assgn3 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  case class BatsmanHist(MatchNum:Int,BatsmanName:String,Team:String,Runs:Int,StrikeRate:Double)
  
  val FileARdd=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/fileA.txt")
  val mapRddA=FileARdd.map(x=>x.split("\t")).map(x=>BatsmanHist(x(0).toInt,x(1),x(2),x(3).toInt,x(4).toDouble))
  
  import spark.implicits._
  
  val BatsmansDf=mapRddA.toDF()
  //BatsmansDf.show(false)
 val batsmenBestRunsAvgHistoryDf= BatsmansDf.groupBy("BatsmanName").agg(avg("Runs").as("AvgRuns"))
 .select("BatsmanName","AvgRuns")
 
 case class BatsmanWC(BatsmanName:String,Team:String)
 
 val FileBRdd=spark.sparkContext.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/fileB.txt")
 
 val mapRddB=FileBRdd.map(x=>x.split("\t")).map(x=>BatsmanWC(x(0),x(1)))
 
 val BatsmanWcDf=mapRddB.toDF()
 
 spark.sql("SET spark.sql.autoBroadcastJoinThreshold =-1")
 
 val joinCon=batsmenBestRunsAvgHistoryDf.col("BatsmanName")===BatsmanWcDf.col("BatsmanName")
 
 val FinalDF=batsmenBestRunsAvgHistoryDf.join(broadcast(BatsmanWcDf),joinCon,"inner")
 .drop(batsmenBestRunsAvgHistoryDf.col("BatsmanName")).orderBy(desc("AvgRuns"))
 
 FinalDF.show(false)

}
