package demo

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object Week10Assignment extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc=new SparkContext("local[*]","Week10Assgn")
  
  val ChapterDataRdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/chapters.csv")
  val chapterMap=ChapterDataRdd.map(x=>(x.split(",")(0).toInt,x.split(",")(1).toInt))

  val ViewsRdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/Asgn_DS/views1.csv,C:/Users/prave/OneDrive/Desktop/Asgn_DS/views2.csv,C:/Users/prave/OneDrive/Desktop/views3.csv")
  val viewsmap=ViewsRdd.map(x=>(x.split(",")(0).toInt,x.split(",")(1).toInt))
  //viewsmap.collect.foreach(println)
  
  val TitleRdd=sc.textFile("C:/Users/prave/OneDrive/Desktop/titles.csv")
  val titlemap=TitleRdd.map(x=>(x.split(",")(0).toInt,x.split(",")(1)))
  
  //Finding no of chapters per course
  val NChap=chapterMap.map(x=>(x._2,1)).reduceByKey(_+_)
  //NChap.collect.foreach(println)
  
  //Removing Dupes from ViewsRdd
  val RemDupViews=viewsmap.distinct()
  val OriginalViews=RemDupViews.map(x=>(x._2,x._1))
  
  //Join ChapterRDD with ViewsRDD based on chapterId to get UserId and CourseID
  
  val joinedRDD=OriginalViews.join(chapterMap)
  //joinedRDD.collect.foreach(println)
  
  //Dropping off chapterID's and append 1 to each 
  val pairRDD=joinedRDD.map(x=>((x._2._1,x._2._2),1))
  val reduce=pairRDD.reduceByKey(_+_)
  //reduce.collect.foreach(println)
  
  //dropping userID and keeping courseId and views
  val courseViewsRdd=reduce.map(x=>(x._1._2,x._2))
  //courseViewsRdd.collect.foreach(println)
  
  //Joining Nchap Rdd and No of Views RDD using courseID
  val newJoinedRDD=courseViewsRdd.join(NChap)
  //newJoinedRDD.collect.foreach(println)
  
  //Calculating % based on tuple to using mapValues
  val CourseCompletionpercentRDD=newJoinedRDD.mapValues( x => (x._1.toDouble/x._2))
  //CourseCompletionpercentRDD.collect.foreach(println)
  
  val formattedpercentageRDD =  CourseCompletionpercentRDD.mapValues(x => f"$x%01.5f".toDouble)
  //formattedpercentageRDD.collect.foreach(println)
  
  //Calculating Scores based on % we get from above CourseCompletionpercentRDD
  val scoresRDD=formattedpercentageRDD.mapValues(x=>{
    if(x>=0.9) 101
    else if(x>=0.5 && x<0.9) 41
    else if(x>=0.25 && x<0.5) 21
    else 1
  })
  
  
  //scoresRDD.collect.foreach(println)
  
  //Calculating which courseID having how many no of views
  val finalScores=scoresRDD.reduceByKey(_+_)
  //finalScores.collect.foreach(println)
  
  val title_score_joinedRDD=finalScores.join(titlemap).map(x=>(x._2._2,x._2._1))
  
  title_score_joinedRDD.sortBy(x=>x._2,false).collect.foreach(println)
}
