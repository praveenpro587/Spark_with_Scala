package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

object Week12Assgn extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name", "DFAssign")
  sparkConf.set("spark.master", "local[*]")
  
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val EmpDf=spark.read
  .format("json")
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/employee.json")
  .load()
  
  val DeptDf=spark.read
  .format("json")
  .option("path", "C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark-Week4/department.json")
  .load()
  
  
  //EmpDf.show(false)
  //DeptDf.show()
  
  val joincon=DeptDf.col("deptid")===EmpDf.col("deptid")
  
  val JoinDf=DeptDf.join(EmpDf,joincon,"left").drop(EmpDf.col("deptid"))
  .select("id","empname","age","deptid","deptName","salary","address")
  
  //JoinDf.show()
  
  //JoinDf.groupBy("deptid").agg(count("id").as("EmpCount"),first("deptName").as("DeptName")).show()
  
  JoinDf.createOrReplaceTempView("EmpTable")
  spark.sql("select deptid,count(empname) as Emp_count,first(deptName) as Dept_name from EmpTable group by deptid order by Emp_count")
  .show
  
  
}
