package readcsv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by xcheng on 3/17/16.
  * 使用databricks提供的spark-csv库读取csv效果不理想
  */
object spkcsvTodf {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("ORDER").setMaster("local[4]")
    val sc = new SparkContext(conf)

    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    //读取csv的两种方式
    import com.databricks.spark.csv._
    //方式一:
    val Order = sqlContext.csvFile(filePath ="/Users/xcheng/Desktop/buybox/Order.csv",useHeader = true,delimiter = ',')
    Order.head(2)
//    //方式二:
//    val options = Map("header"->"true","path"->"/Users/xcheng/Spark/ideaProj/ScalaDataAnalysisCookbook_Code/Chapter1-spark-csv/StudentData.csv")
//    val newOrder = sqlContext.load("com.databricks.spark.csv",options)
    //打印DataFrame的schema
    Order.printSchema()

//
//    //使用SQL查询
    Order.registerTempTable("Ordertable")
//    val dfFiltereBySQL = sqlContext.sql("select * from Ordertable where KIND=1 ORDER BY \"SALE_BASIS_DY\" desc")
//    dfFiltereBySQL.show(7)
    sqlContext.sql("select distinct(SALE_TYPE) saletype from Ordertable").show()

    //Cancellation_order.csv
//    val Cancellation: DataFrame = sqlContext.csvFile(filePath = "/Users/xcheng/Desktop/buybox/Cancellation_order.csv",useHeader = true,delimiter = ',')
//
//    //join 两个 dataframe
//    val Order_and_Cancell: DataFrame = Order.join(Cancellation,Order("ORDERID")===Cancellation("ORDERID"))
//    Order_and_Cancell.show(10)



  }

}
