package demo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object UnionAndUnionBy extends App{
  
  val SparkConf=new SparkConf()
  
  SparkConf.set("spark.app.name", "UnionAndUnionBy")
  SparkConf.set("spark.master", "local[*]")
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  val spark=SparkSession.builder()
  .config(SparkConf)
  .getOrCreate()
  
  val list1=List(Row(587,"Praveen",90))
  val list2=List(Row("Prakash",580,95))
  
  val list1Rdd=spark.sparkContext.parallelize(list1)
  val list2Rdd=spark.sparkContext.parallelize(list2)
  
  val list1Schema=StructType(List(
      StructField("ID",IntegerType,false),
      StructField("Name",StringType,true),
      StructField("Marks",IntegerType,true)      
      ))
  
  val list2Schema=StructType(List(
      StructField("Name",StringType,false),
      StructField("ID",IntegerType,true),
      StructField("Marks",IntegerType,true)      
      ))
      
   val df1=spark.createDataFrame(list1Rdd, list1Schema)
   //df1.show()
   
   val df2=spark.createDataFrame(list2Rdd, list2Schema).select("ID","Name","Marks")
   
   //df2.show()
   
   /*Here we have same no of columns but columns 
   are not in order so if we use union we need to interchange columns
   before performing union*/
   val df3=df1.union(df2)
   
   df3.show()
   
   /*No need of doing extra work like select on df we can use 
    UnionByName
    */
   val df4=spark.createDataFrame(list2Rdd, list2Schema)
   val df5=df1.unionByName(df4)
   df5.show()
   
   /* Example we have non equal dataframes 
    (i.e) No of columns are not equal that 
    case Union won't work we need to use 
    UnionByName
    */
   
   val list3=List(Row("Ravi",580,95,"CSE"))
   val list3Rdd=spark.sparkContext.parallelize(list3)
   val list3Schema=StructType(List(
      StructField("Name",StringType,false),
      StructField("ID",IntegerType,true),
      StructField("Marks",IntegerType,true),
      StructField("Branch",StringType,true)
      ))
      
  val df6=spark.createDataFrame(list3Rdd, list3Schema)
  
  val cols1 = df1.columns.toSet
  val cols2 = df6.columns.toSet
  val total = cols1++cols2 // union

  def expr(myCols: Set[String], allCols: Set[String]) = 
  {
   allCols.toList.map(x => x match {
      case x
      if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
     })
  }
  
  val finalDF=df1.select(expr(cols1, total): _ * ).unionByName(df6.select(expr(cols2, total): _ * ))
  
  finalDF.show()
  
  /*In spark3 we can directly use as Below
   val finalDF=df1.unionByName(df6,allowMissingColumns=true)
   */
  
}
