package BasicPrograms
import java.util.regex.Pattern
object Pattern1 {
  def main(args:Array[String]){
    /*var i=0
    for(i<- 0 to 2){
      var j=0
      for(j<-0 to 2){
        if(i%2==0){
          println("1")
        }
        else
          println("0")
        
      }
      println()
    }*/
    var row=4
    for(i<-0 to row){
      if(i==Math.floor(row/2)){
        println(i)
      }
    }
  }
}
