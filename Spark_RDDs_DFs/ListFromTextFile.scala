package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object ListFromTextFile extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","List")
  
  val fileRdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/bigLog.txt")
  val mapRdd=fileRdd.map(x=>(x.split(":")(0),1))
  val reduce=mapRdd.reduceByKey(_+_)
  reduce.collect.foreach(println)
  
  val myList=List("ERROR: Thu Jun 04 10:37:51 BST 2015",
                   "WARN: Sun Nov 06 10:37:51 GMT 2016",
                   "WARN: Mon Aug 29 10:37:51 BST 2016",
                   "ERROR: Thu Dec 10 10:37:51 GMT 2015",
                   "ERROR: Fri Dec 26 10:37:51 GMT 2014",
                   "ERROR: Thu Feb 02 10:37:51 GMT 2017",
                   "WARN: Fri Oct 17 10:37:51 BST 2014",
                   "ERROR: Wed Jul 01 10:37:51 BST 2015",
                   "WARN: Thu Jul 27 10:37:51 BST 2017")
                   
  val par=sc.parallelize(myList)
  val finalres=par.map({x=>
    val cols=x.split(":")
    val split=cols(0)
    (split,1)
  }).reduceByKey(_+_)
  finalres.collect.foreach(println)
  
}

