package BasicPrograms
import scala.io.StdIn._
object StringPalindrome {
  def main(args:Array[String]){
    var str=readLine
    /*var str1=str.reverse
    if(str==str1)
      println(s"$str is palindrome")
    else
      println(s"$str is not a palindrome")*/
    
    var str1=""
    var str2=""
    for(i<-str){
      str1=i+str1
    }
    println(str1)
    if(str==str1)
      println(s"$str is palindrome")
    else
      println(s"$str is not a palindrome")
      
    for(i<-str.length()-1 to 0 by -1){
      str2+=str.charAt(i)
    }
    println(str2)
    
  }
}
