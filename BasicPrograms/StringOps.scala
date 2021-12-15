package BasicPrograms
import scala.io.StdIn._
object StringOps {
  def main(args:Array[String]){
    val str=readLine
    /*val str1=readLine
    println(str.contains('e'))
    println(str.length())
    println(str.concat(" Kumar"))
    println(str.replace('e', 'a'))
    
    println(str.contains(str1))
    println(str.equals(str1))*/
    val ch="e"
    var count=0
    for(i<- 0 to str.length()-1){
      if(ch.contains(String.valueOf(str.charAt(i)))){
        count+=1
      }
    }
    println(count)
  }
}
