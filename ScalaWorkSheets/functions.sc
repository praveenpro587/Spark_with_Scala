object functions {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  def squareIt(x:Int):Int={
  	return x*x
  }                                               //> squareIt: (x: Int)Int
  println(squareIt(2))                            //> 4
  
  val name="praveen"                              //> name  : String = praveen
  def greeting(a:String):String={
  	return a
  }                                               //> greeting: (a: String)String
  println(greeting(s"Hello $name"))               //> Hello praveen
  
  def transformx(x:Int,f:Int=>Int):Int={
  	f(x)
  }                                               //> transformx: (x: Int, f: Int => Int)Int
  
  println(transformx(2,squareIt))                 //> 4
  
  
  transformx(2,x=>{val y=x*2;y*y})                //> res0: Int = 16
  
  var a= 1 to 10                                  //> a  : scala.collection.immutable.Range.Inclusive = Range 1 to 10
  a.map(x=>x*2)                                   //> res1: scala.collection.immutable.IndexedSeq[Int] = Vector(2, 4, 6, 8, 10, 12
                                                  //| , 14, 16, 18, 20)
  
  def norfac(m:Int):Int={
  
  	var res=1
  	for(i <- 1 to m){
  		res=res*i
  	}
  	res
  }                                               //> norfac: (m: Int)Int
  norfac(5)                                       //> res2: Int = 120
  
  
  
  def recurfact(n:Int):Int=
  {
  	if(n==0||n==1)
  		1
  	else
  		n*recurfact(n-1)
  }                                               //> recurfact: (n: Int)Int
  recurfact(5)                                    //> res3: Int = 120
  
 def tailFact(n:Int,result:Int):Int={
 	
 	if(n==1) result
 	else tailFact(n-1,result*n)
 
 }                                                //> tailFact: (n: Int, result: Int)Int
 tailFact(5,1)                                    //> res4: Int = 120
 
 def areaOfCircle={val pi=3.14; (x:Int)=>(pi*x*x)}//> areaOfCircle: => Int => Double
 
 areaOfCircle(10)                                 //> res5: Double = 314.0
 
 val c:Int='a'                                    //> c  : Int = 97
 println(c)                                       //> 97
 
 val i=1 to 100                                   //> i  : scala.collection.immutable.Range.Inclusive = Range 1 to 100
 i.reduce((x,y)=>x+y)                             //> res6: Int = 5050
 
 def cubeIt(x:Int):Int={
  	return x*x*x
  }                                               //> cubeIt: (x: Int)Int
 
 def genericFunc(x:Int,y:Int,f:Int=>Int)={
 
 f(x)+f(y)
 }                                                //> genericFunc: (x: Int, y: Int, f: Int => Int)Int
 
 genericFunc(2,3,cubeIt)                          //> res7: Int = 35
 
 
 def genericFunc1(f:Int=>Int)(x:Int,y:Int)={
 
 f(x)+f(y)
 
 }                                                //> genericFunc1: (f: Int => Int)(x: Int, y: Int)Int
 
 val sumofSquares=genericFunc1(x=>x*x)_           //> sumofSquares  : (Int, Int) => Int = functions$$$Lambda$21/1638172114@39fb3a
                                                  //| b6
 sumofSquares(4,5)                                //> res8: Int = 41
 
 
 
}
