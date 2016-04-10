package LearnScala

/**
  * Created by xcheng on 3/18/16.
  */
object ArrayList {
  def main(args: Array[String]) {
    val numberArray = new Array[Int](10)
    val strArray = new Array[String](10)

    strArray(0) = "First Element"
    strArray.foreach(println)

    val strArray2 = Array("First","Second")
    strArray2.foreach(print)

    //变长数组
    import scala.collection.mutable.ArrayBuffer
    val strArrayVar = ArrayBuffer[String]()
    strArrayVar+="Hello"
    strArrayVar+=("World","Programmer")
    strArrayVar.foreach(print)

    strArrayVar++=Array("Wilcome","To","scala")
    strArrayVar.foreach(print) ;println
    strArrayVar++=List("Wellcome","To","spark")
    strArrayVar.foreach(print) ;println()

    strArrayVar.trimEnd(3)
    strArrayVar.foreach(print)

    var intArrayVar = ArrayBuffer(1,1,2)
    intArrayVar.insert(0,6)     //0处开始插入6
    intArrayVar.insert(1,7,8,9)    //1处开始插入8,9
    intArrayVar.foreach(print)

    intArrayVar.remove(0,4)     //0处开始删除4个
    intArrayVar.toArray.toBuffer
    //遍历
    for(i <-0 to intArrayVar.length-1)
      println("Array Element: "+intArrayVar(i))
    for(i <-0 until  intArrayVar.length) println("Array Element: "+intArrayVar(i))
    for(i <-intArrayVar) println("Array Element: "+i)
    for(i <-0 until  (intArrayVar.length,2)) println("Array Element: "+intArrayVar(i))
    for(i <-(0 until  intArrayVar.length).reverse) println("Array Element: "+intArrayVar(i))//倒序打印
    //转换
    var intArrayVar2 = for (i<- intArrayVar) yield i*2
    intArrayVar2.foreach(println)

    var intArrayNoBuffer = Array(1,2,3)
    var intArrayNoBuffer2 = for (i<- intArrayNoBuffer) yield i*2
    intArrayNoBuffer2.foreach(println)

    var intArrayNoBuffer3 = for(i<-intArrayNoBuffer if i>=2) yield i*2
    intArrayNoBuffer3.foreach(println)

    //数组操作算法
    val intArr = Array(1,2,3,4,5,6,7,8,9,10)
    print (intArr.sum)
    print (intArr.max)
    ArrayBuffer("Hello","Hell","Hey","Happy").max





    }

}
