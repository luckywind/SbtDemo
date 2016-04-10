package readcsv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by xcheng on 3/17/16.
  */
object csvtest {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("csvDataFrame").setMaster("local[4]")
    val sc = new SparkContext(conf)

    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    //读取csv的两种方式
    import com.databricks.spark.csv._
    //方式一:
    val students = sqlContext.csvFile(filePath ="/Users/xcheng/Spark/ideaProj/ScalaDataAnalysisCookbook_Code" +
      "/Chapter1-spark-csv/StudentData.csv",useHeader = true,delimiter = '|')
    students.head(2)
    //方式二:
    val options = Map("header"->"true","path"->"/Users/xcheng/Spark/ideaProj/ScalaDataAnalysisCookbook_Code/Chapter1-spark-csv/StudentData.csv")
    val newStudents = sqlContext.load("com.databricks.spark.csv",options)
    //打印DataFrame的schema
    students.printSchema()

    students.show(3)

    students.head(5).foreach(println)

    val emailDataFrame = students.select("email")
    emailDataFrame.show(3)

    val studentsEmailDF = students.select("studentName","email")
    studentsEmailDF.show(3)

    students.filter("id>5").show(7)

    students.filter("studentName = ''").show(7)

    students.filter("studentName = '' OR studentName = 'NULL'").show(7)

    students.filter("SUBSTR(studentName,0,1) = 'M'").show(7)

    students.sort(students("studentName").desc).show(7)

    students.sort("studentName","id").show(10)

    //列重命名
    val copyofStudents = students.select(students("studentName").as("name"),students("email"))
    copyofStudents.show()

    //使用SQL查询
    students.registerTempTable("students")
    val dfFiltereBySQL = sqlContext.sql("select * from students where studentName!=' ' order by email desc")
    dfFiltereBySQL.show(7)

    //表内联
     val students1 = sqlContext.csvFile("/Users/xcheng/Spark/ideaProj/ScalaDataAnalysisCookbook_Code/Chapter1-spark-csv/StudentPrep1.csv"
      ,useHeader = true,delimiter = '|')
    val students2 = sqlContext.csvFile("/Users/xcheng/Spark/ideaProj/ScalaDataAnalysisCookbook_Code/Chapter1-spark-csv/StudentPrep2.csv"
      ,useHeader = true,delimiter = '|')
    val studentsJoin = students1.join(students2,students1("id")===students2("id"))
    studentsJoin.show(studentsJoin.count.toInt)
    //右链接
    val studentsRightOuterJoin = students1.join(students2,students1("id")===students2("id"),"right_outer")
    studentsRightOuterJoin.show(studentsRightOuterJoin.count.toInt)
    //保存数据
    val saveoptions=Map("header"->"true","path"->"/Users/xcheng/Spark/ideaProj/ScalaDataAnalysisCookbook_Code/Chapter1-spark-csv/mystudent.csv")
    val saveofStudents=students.select(students("studentName").as("name"),students("email"))

    saveofStudents.save("com.databricks.spark.csv",SaveMode.Overwrite,options)


  }

}
