package data.Category


import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}
/**
  * Created by xcheng on 3/21/16.
  *
  */
object Order {
  def main(args:Array[String]){
    val conf=new SparkConf().setAppName("OrderLinearRegression")//.setMaster("local[2]")
    val sc=new SparkContext(conf)


    //数据预处理
    //去掉列名行:sed 1d train.tsv >train_noheader.tsv
    val rawData=sc.textFile("/Users/xcheng/Desktop/buybox/Order_noheader.csv")
    val records=rawData.map(_.split(",")) //按tab分隔
    //记录的第四列为类别型变量,重新编码映射到数字,得到一个类别到序号的Map
    val category=records.map(r=>r(3)).distinct().collect().zipWithIndex.toMap
   // category.foreach(print)  //("D",0)("S",1)



    val data=records.map{point=>
      val replaceData=point.map(_.replaceAll("\"",""))//去掉引号
    val label=replaceData(7).toInt //第八个字段是类别标签或者因变量
    val categoriesIndex=category(point(3))//把第三列类别变量通过上面的MAP映射到数字
    val categoryFeatures=Array.ofDim[Double](category.size)//类别特征,初始全为0
      categoryFeatures(categoriesIndex)=1.0 //相应类别编码为1

      //val otherfeatures=replaceData.slice(4,replaceData.size-1).map(x=>if(x=="?") 0.0 else x.toDouble)
      val otherfeatures=(replaceData.slice(0,3)++replaceData.slice(4,7)).map(x=>x.toDouble)
      val features=otherfeatures++categoryFeatures//添加类别标签,两个RDD联合需要用++
      LabeledPoint(label,Vectors.dense(features))
    }.cache()
    print("data.first-------------------------------------------"+data.first())




    //特征标准化
    val vectors=data.map(p=>p.features) //获得特征向量
    val scaler=new StandardScaler(withMean = true,withStd = true).fit(vectors)//创建一个标准化对象,其创建需要fit一下要标准化的特征
    val scalerData=data.map(point=>
      LabeledPoint(point.label,scaler.transform(point.features))
    )




    //构建模型
    /*
    逻辑回归模型,使用SGD需要0-1label,使用LogisticRegressionWithLBFGS不需要
     */
    /*
    val lrModel=LogisticRegressionWithSGD.train(scalerData,10)

    //模型评价
    //(1)二分类型评价
    val predictrueData=scalerData.map{point=>
      if(lrModel.predict(point.features)==point.label) 1 else 0
    }.sum()
    val accuracy=predictrueData/data.count()
    println(accuracy)
    */
    /*
   线性模型
     */
    //线性回归
    val regression = new LinearRegressionWithSGD().setIntercept(true)
    regression.optimizer.setNumIterations(1000).setStepSize(0.1)
    val model = regression.run(data)



    //(2)拟合型评价
    val predictions = model.predict(data.map(point=>point.features))
    val actuals = data.map(_.label)
    val predictionsAndActuals  =predictions.zip(actuals)
    val sumSquaredErrors = predictionsAndActuals.map{case (pred,act)=>
      //println(s" act,pred and difference $act,$pred ${act-pred}")
      math.pow(act-pred,2)
    }.sum()
    val meanSquaredError = sumSquaredErrors/data.count()
    println(s"SSE is $sumSquaredErrors")
    println(s"MSE is $meanSquaredError")
    println("model weight:"+model.weights)
  }
}
