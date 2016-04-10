//默认使用immutdde集合,如果使用mutble集合需要引包
import scala.collection.mutable
val mutableSet = mutable.Set(1,2,3)
val immutableSet = Set(1,2,3)

//Set
val numsSet = Set(3.0, 5)
numsSet+6
for(i<-numsSet+6)println(i)

val linkedHashSet = scala.collection.mutable.LinkedHashSet(3.0,5)
linkedHashSet+6

//KeyValue
val studentInfo =Map("john"->21,"stephen"->22,"lucy"->20)
//studentInfo.clear()不可变因此不可清空
val studentInfoMutable = scala.collection.mutable.Map("john"->21,"stephen"->22,"lucy"->20)
for(i<- studentInfoMutable)println(i)

studentInfoMutable.foreach(e=>println(e._1+":"+e._2))
val xMap =scala.collection.mutable.Map(("spark",1),("hive",1))
"spark"->1
xMap.get("spark")
xMap.get("sparkSQL")

//元祖
("hello","china","beijing")
val tuple= ("hello","chian",1)
tuple._1
tuple._2
tuple._3

val (first,second,third)=tuple

//队列
var queue=scala.collection.immutable.Queue(1,2,3)
queue.dequeue
queue.enqueue(4)

var queue1 = scala.collection.mutable.Queue(1,2,3,4,5)
queue1 +=5
queue1 ++=List(6,7,8)

//栈
import scala.collection.mutable.Stack
val stack = new Stack[Int]
val stack1 = Stack(1,2,3)
stack1.top
stack.push(1)
stack.push(2)
stack.top
stack