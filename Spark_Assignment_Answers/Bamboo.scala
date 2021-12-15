//Solving 3 Datasets based on Requirement 

package demo

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType,StructType,StructField,IntegerType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._

object Bamboo extends App{
  
  def parseLine(Line:String)={
    val fields=Line.split("\\|")
    val SNO=fields(0).toInt
    val District=fields(1).replaceAll("[^a-zA-Z]", "")
    val Area=fields(2).replaceAll("[^0-9NA]", "")
    val Production=fields(3)
    (SNO,District,Area,Production)
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
 
  val sc=new SparkContext("local[*]","Bamboo")
  
  val spark:SparkSession=SparkSession.builder()
      .master("local[*]")
      .appName("RddCreation")
      .getOrCreate()
      
  val schema = StructType( Array(
                 StructField("SNO", IntegerType,true),
                 StructField("District", StringType,true),
                 StructField("Area", StringType,true),
                 StructField("Production", StringType,true)
  ))
    
  import spark.implicits._
  
  //Bamboo Dataset Implementation
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/SanjayAssign/Bamboo_plantation.txt")
  val maprdd=filerdd.map(parseLine)
  val rowRDD=maprdd.map(x=>Row(x._1,x._2,x._3,x._4))
  val df=spark.createDataFrame(rowRDD,schema).withColumn("Productivity",col("Production")/col("Area"))
  df.show()//Printing DF of Bamboo Dataset
  df.createOrReplaceTempView("Bamboo")//Creating view
  val sqlDF = spark.sql("SELECT min(Production),max(Production),sum(Area) FROM Bamboo")//Writing Query on View
  sqlDF.show()//Printing Query DF
  
  //Rubber Dataset Implementation
  val filerdd2=sc.textFile("C:/Users/prave/OneDrive/Desktop/SanjayAssign/Rubber_plantation.txt")
  val header=filerdd2.first()
  val data=filerdd2.filter(x=>x!=header)
  val maprdd1=data.map(parseLine)
  val rowRDD1=maprdd1.map(x=>Row(x._1,x._2,x._3,x._4))
  val df1=spark.createDataFrame(rowRDD1, schema).withColumn("Productivity",col("Production")/col("Area"))
  df1.show()//printing DF of rubber dataset
  df1.createOrReplaceTempView("Rubber")//creating Temp View
  val sqlDF1 = spark.sql("SELECT min(Production),max(Production),sum(Area) FROM Rubber")//Checking min,max-->Producton,Sum of Area
  sqlDF1.show()//Printing Query DF
  
  //Tea Dataset implementation
  val df2 = spark.read.option("delimiter", "|").csv("C:/Users/prave/OneDrive/Desktop/SanjayAssign/Tea_plantation.txt")
  val names : Array[String] = Array("SNO", "District", "Area","Production","Productivity")
  def splitCSV(dataFrame: DataFrame, columnNames : Array[String], spark: SparkSession) : DataFrame = {
    val columns = dataFrame.columns
    var finalDF : DataFrame = Seq.empty[(String,String,String,String,String)].toDF(columnNames:_*)
    //Implementation of 5th delimeter reading of Dataset
    for(order <- 0 until(columns.length) -5 by(5) ){
      finalDF = finalDF.union(dataFrame.select(col(columns(order)).as(columnNames(0)),
                                               col(columns(order+1)).as(columnNames(1)),
                                               col(columns(order+2)).as(columnNames(2)),
                                               col(columns(order+3)).as(columnNames(3)),
                                               col(columns(order+4)).as(columnNames(4))
                                              ))
    }
    finalDF
  }
val finalDF = splitCSV(df2, names, spark)
finalDF.show(false) //Printing DF of Tea Dataset
  


//Union Operation Between Bamboo and Rubber DF's
val df3=df.union(df1)
df3.show()
  
}
