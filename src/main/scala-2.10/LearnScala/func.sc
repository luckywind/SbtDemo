val increase = (x:Int)=>x+1
increase(10)

def increaseAnother(x:Int)=x+1
increaseAnother(10)

val increase2=(x:Int)=>{
 println("Xue")
  println("Xuedf")
  println("Xusfde")
  println("Xuesfsdfsd")
  x+1
}

increase2(10)

Array(1,2,3,4).map(increase).mkString(",")
Array(1,2,3,4).map{(x:Int)=>x+1}.mkString(",")

val fun1=1+(_:Double)
fun1(999)

val fun2:(Double)=>Double=1+_
fun2(200)

//函数参数
def convertIntToString(f:(Int)=>String)=f(4)
convertIntToString((x:Int)=>x+"s")
