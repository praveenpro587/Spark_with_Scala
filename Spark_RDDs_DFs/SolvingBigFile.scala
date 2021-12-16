package demo
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf



object SolvingBigFile extends App{
  
  case class Logging(Level:String,DateTime:String)
  
  def mapper(line:String):Logging={
    val fields=line.split(",")
    
    val logging:Logging=Logging(fields(0),fields(1))
    return logging
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val list=List(("DEBUG,2015-2-6 16:24:07"),("WARN,2016-7-26 18:54:43"),
                ("FATAL,2015-5-10 03:58:42"),("WARN,2016-7-18 19:42:18"),
                ("INFO,2017-8-20 13:17:27"),("INFO,2015-8-13 09:28:17"))
                
 val rdd=spark.sparkContext.parallelize(list)
 
 val maprdd=rdd.map(mapper)
 
 import spark.implicits._
 
 val df=maprdd.toDF()
 
 df.createOrReplaceTempView("LogTable")
 
 //spark.sql("select Level,count(DateTime) from LogTable group by Level order by Level").show(false)
 
 val newdf=spark.sql("select Level,date_format(DateTime,'MMMM') as Month from LogTable")
 
 newdf.createOrReplaceTempView("NewDF")
 
 spark.sql("select Level,Month,count(1) as Count from NewDF group by Level,Month order by Count").show
 
 
}
