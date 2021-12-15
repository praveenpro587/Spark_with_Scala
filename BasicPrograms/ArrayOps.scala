package BasicPrograms

import java.util.Arrays;

object ArrayOps {
  def main(args:Array[String]){
    val arr=Array(1,2,3,4,2,3)
    /*println(arr.length)
    println(arr.indexOf(2))
    println(arr.count(_==2))
    println(arr.min)
    println(arr.max)
    println(arr.sum)
    println(arr.sum/arr.length)*/
    val arr2=Array(1,2,3,4)
    if(arr.equals(arr2))
      println("Both are equal")
    else
      println("Both are not same")
    var arr3=new Array[Int](4)
    arr3 = Arrays.copyOf(arr2,2);
    for(i<-0 to arr3.length-1){
     println(arr3(i)) 
    }
    var arr4=arr.clone().take(3)
    for(i<-0 to arr4.length-1){
      println(arr4(i))
    }
    
    /*for(i<-arr.length-1 to 0 by -1){
      println(arr(i))
    }*/
  }
}
