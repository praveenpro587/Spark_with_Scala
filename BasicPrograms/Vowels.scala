package BasicPrograms
import scala.io.StdIn._
object Vowels {
  def main(args:Array[String]){
    var str=readLine
    var vow="aeiou"
    /*var count=0
    for(i<-vow){
      if(str.contains(i)){
        count+=1
      }
    }
    println(count)*/
    
    for (i<-vow){
      if(str.contains(i)){
        println(i)
      }
    }
  }
  
}
