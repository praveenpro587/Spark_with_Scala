package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object SolveBigFileReal extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val FileDF=spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path", "C:/Users/prave/OneDrive/Desktop/biglog.txt")
  .load()
  
  FileDF.createOrReplaceTempView("LogTable")
  
  val DF1=spark.sql("""select level,date_format(datetime,'MMMM') as Month,
    count(1) as Total from LogTable group by level,Month order by Total""")
    
    
 val result=spark.sql("""select level,date_format(datetime,'MMMM') as Month,
            cast(first(date_format(datetime,'M')) as int) as monthnum,
            count(1) as Total from LogTable group by level,Month order by monthnum,level""").drop("monthnum")
            
           //Not Optimized one
            spark.sql("""select level,date_format(datetime,'MMMM') as Month,
            cast(date_format(datetime,'M') as int) as monthnum
            from LogTable""").groupBy("level").pivot("monthnum").count()
            
 //Optimized one using Pivot  
 val Columns =List("January","February","March","April","May","June","July","August","September","October","November","December")  
 spark.sql("""select level,date_format(datetime,'MMMM') as Month
            from LogTable""").groupBy("level").pivot("Month",Columns).count().show(60)
 
  
}
