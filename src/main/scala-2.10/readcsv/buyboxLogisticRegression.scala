package readcsv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by xcheng on 3/17/16.
  * 易于选取指定列,使用Spark原生解析类别变量,过程复杂......
  *
  * 尝试把RDD转为DF 并注册为table
  */
object buyboxLogisticRegression {

  case class Order(id:Int,srl:Int,kind:Int,Type:String)

  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("buybox").setMaster("local[4]")
    val sc = new SparkContext(conf)

    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //数据准备
    val rawOrder=sc.textFile("/Users/xcheng/Desktop/buybox/Order.csv")
    //  print (rawTrainningData.count())//9381001条

    //过滤首行列名
    def isColumnNameLine(line:String):Boolean = {
      if (line != null && line.contains("SALE_BASIS_DY"))true
      else false
    }
    //#去除引号,取指定字段得到DF,可以注册为table
    val parsedOrder = rawOrder.filter(!isColumnNameLine(_))
      .map(line=>{
        val splits=line.replace("\"","").split(",")//.slice(1,4)//.foreach(_.subSequence(1,6))
        val index=Array(0,1,2,3) //取指定字段,第四个字段为字符型
        index collect splits
      }).map(p=>Order(p(0).toInt,p(1).toInt,p(2).toInt,p(3))).toDF("id","srl","kind","Type")
    parsedOrder.show(3)

    parsedOrder.registerTempTable("parsedOrderTable")

    //类别变量编码
    val indexer = new StringIndexer().setInputCol("Type").setOutputCol("Typeindex").fit(parsedOrder)
    val indexed = indexer.transform(parsedOrder)
    val encoder = new OneHotEncoder().setInputCol("Typeindex").setOutputCol("newType")
    val encodedOrder = encoder.transform(indexed)
    encodedOrder.select("id","srl","kind","newType").show(3)


//    //执行LogisticRegression
//    val trainningdata=encodedOrder.map{line =>
//      val featureVector =Vectors.dense(line.)
//    }
  }

}
