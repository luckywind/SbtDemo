var multiDimArr = Array(Array(1,2,3),Array(2,3,4))
multiDimArr(0)(2)
for(i<-multiDimArr)println( i.mkString(","))
//List 一旦创建不可改变
val fruit = List("Apple","Banana","Orange")
val nums = List(1,2,3,4,5)
val diagMatrix = List(List(1,0,0),List(0,1,0),List(0,0,1))
for(i <-nums)println("List Element: "+i)
//List具有递归结构,和其他数据集合一样具有协变性,
//即S是T的子类型,则List[S]也是List[T]的子类型
var listStr:List[Object]= List("This","Is","Covariant","Example")
var listStr1= List()
var listStr2:List[String]=List()

//List常用构造方法法
val num = 1 :: (2 :: (3 :: (4 :: Nil)))
val num1 = 1 :: 2 :: 3::4::Nil
nums.isEmpty
num1.head
num1.tail
num1.tail.head
def isort(xs:List[Int]):List[Int] =
if(xs.isEmpty)Nil
else insert(xs.head,isort(xs.tail))

def insert(x:Int,xs:List[Int]):List[Int] =
if(xs.isEmpty|| x<= xs.head)x :: xs
else xs.head :: insert(x,xs.tail)

List(1,2,3):::List(4,5,6)

nums.init
nums.last
nums.reverse
nums.drop(3)
nums.take(3)
nums.splitAt(2)
(nums.take(2),nums.drop(2))
//
val newnum = List(1,2,3,4)
val chars = List('1','2','3','4')
newnum zip chars
newnum.toString
newnum.mkString
newnum.toArray
//List 伴生对象方法
List.apply(1,2,3)
List.range(2,6)
List.range(2,6,2)
List.range(2,6,-1)
List.range(6,2,-1)
List.make(5,"hey")
val xss =List(List('a','b'),List('c'),List('d','e'))
xss.flatten

List.concat(List('a','b'),List("c"))