/*
http://twitter.github.io/scala_school/zh_cn/collections.html
 */
val numbers1 = List(1,2,3,4)

Set(1,2,3)

val hostPort = ("localhost",80)
hostPort._1
//元组与模式匹配
hostPort match{
  case ("localhost",port) =>port
  case (host,port) =>(host,port)
}

//zip
List(1,2,3).zip(List("a","b","c"))
//partition
val numbers = List(1,2,3,4,5,6,7,8,9,10)
numbers.partition(_ %2==0)

//find
numbers.find((i:Int)=> i>5)

//drop 去掉前n个元素
numbers.drop(5)
numbers.dropWhile(_ % 2!=0)

//foldLeft
numbers.foldLeft(0)((m:Int,n:Int)=> m+n)

//flatMap
val nestedNumbers= List(List(1,2),List(3,4))
nestedNumbers.flatMap(x=>x.map(_ *2))
nestedNumbers.map((x:List[Int]) => x.map(_ *2)).flatten