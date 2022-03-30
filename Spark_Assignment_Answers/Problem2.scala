package ProblemSolving

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Problem2 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val spark: SparkSession = SparkSession.builder().appName("DataValidation").master("local").getOrCreate()
   
   val data = Seq("A","A","A","B","B","C","D")
   val rdd = spark.sparkContext.parallelize(data)
   val rdd2 = rdd.map(x=>(x,1)).reduceByKey((x,y)=>(x+y))
   val df = spark.createDataFrame(rdd2).toDF("C1","C2")
   val t1 = df.select(df("c1")).where(df("c2")==="1")
   val t2 = df.select(df("c1")).where(df("c2")>"1")
   
   import spark.implicits._
   val rdd1 = rdd.toDF("rd1")
   //rdd1.show()
   val t3 = rdd1.join(t2,rdd1("rd1")===t2("c1"),"inner").select("c1")
   
  t1.show()
  t2.show()
  t3.show()
   
   
}
