package BasicPrograms
import scala.io.StdIn._
object Table {
  def main(args:Array[String]){
    val n=readInt()
    val m=readInt()
    
    for(i<-1 to m){
      println(s"$n*$i ="+n*i)
    }
  }
}
