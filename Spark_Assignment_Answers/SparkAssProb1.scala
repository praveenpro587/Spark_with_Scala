package demo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object SparkAssProb1 extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","Assgn1")
  
  val filerdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/Trendy Tech/Spark/student.txt")
  /*val agerdd=filerdd.map(x=>x.split(",")(1).toInt)
  for(i<-agerdd){
    if(i>18) println(s"$i Y")
    else println(s"$i N")
  }*/
  
  val agerdd=filerdd.map(x=>{
    val fields=x.split(",")
    if(fields(1).toInt>18)
       (fields(0),fields(1),fields(2),"Y")
    else
      (fields(0),fields(1),fields(2),"N")
  })
  agerdd.collect.foreach(println)
}
