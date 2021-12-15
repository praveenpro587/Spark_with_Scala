object ClassLearning {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  object Person{
  
  val N_eyes=2
  def isFly:Boolean=false
  }
  
  class Person(val name:String,val age:Int){
  //instance level functionality
  def getName=name
  def getAge=age
  
  }
  //campanions
  val p=new Person("praveen",26)                  //> p  : ClassLearning.Person = ClassLearning$Person$3@380fb434
  val p1=new Person("sanjay",25)                  //> p1  : ClassLearning.Person = ClassLearning$Person$3@668bc3d5
  p.getName                                       //> res0: String = praveen
  p.getAge                                        //> res1: Int = 26
  p1.getName                                      //> res2: String = sanjay
  p1.getAge                                       //> res3: Int = 25
  Person.N_eyes                                   //> res4: Int = 2
  Person.isFly                                    //> res5: Boolean = false
}
