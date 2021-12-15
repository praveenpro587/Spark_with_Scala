package BasicPrograms

import scala.util.control.Breaks._

object WordCount {
  def main(args:Array[String]){
    val str="praveeen"
    //println(str.count(_=='l'))
    var str1=str.distinct
    println(str1)
    for(i<-str1){
      //println(i)
      var st=str.count(_==i)
      println(s"count of $i is $st")
    }
  }
}
