//Spark-Streaming WordCount Program
//Type this command in another terminal of cloudera nc -lk 9998
//open spark shell using spark-shell --master local[2]

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamContext._


val ssc=new StreamingContext(sc,Seconds(5))
val lines=ssc.socketTextStream("localhost",9998)
val words=lines.flatMap(x=>x.split(" "))
val map=words.map(x=>(x,1))
val WC=map.reduceByKey(_+_)
WC.print()
WC.print()
