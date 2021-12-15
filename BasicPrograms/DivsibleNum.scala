package BasicPrograms
import scala.io.StdIn._
object DivsibleNum {
  
  def main(args:Array[String]){
    val n=readInt()
    val m=readInt()
    for(i<-0 to m){
      if(i%n==0){
        println(i)
      }
    }
  }
}
