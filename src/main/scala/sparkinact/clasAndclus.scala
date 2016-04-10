package sparkinact

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer
//import sqlContext.implicits._
import org.apache.spark.sql.types.{StructType,StructField,StringType,DoubleType}
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.classification.OneVsRest
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.CategoricalSplit
import org.apache.spark.ml.tree.ContinuousSplit
import org.apache.spark.ml.tree.InternalNode
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.clustering.KMeans


/**
  * Created by xcheng on 4/9/16.
  */
object clasAndclus {
  def main(args: Array[String]) {
  /*  val conf: SparkConf = new SparkConf().setAppName("clasAndclus").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)

    val census_raw = sc.textFile("/path/to/adult.raw", 4).map(x => x.split(", ")).
      map(row => row.map(x => try { x.toDouble } catch { case _ => x }))

    val adultschema = StructType(Array(
      StructField("age",DoubleType,true),
      StructField("workclass",StringType,true),
      StructField("fnlwgt",DoubleType,true),
      StructField("education",StringType,true),
      StructField("marital_status",StringType,true),
      StructField("occupation",StringType,true),
      StructField("relationship",StringType,true),
      StructField("race",StringType,true),
      StructField("sex",StringType,true),
      StructField("capital_gain",DoubleType,true),
      StructField("capital_loss",DoubleType,true),
      StructField("hours_per_week",DoubleType,true),
      StructField("native_country",StringType,true),
      StructField("income",StringType,true)
    ))
    val dfraw = sqlContext.applySchema(census_raw.map(Row.fromSeq(_)), adultschema)
    dfraw.show()

    dfraw.groupBy(dfraw("workclass")).count().foreach(println)
    //Missing data imputation
    val dfrawrp = dfraw.na.replace(Array("workclass"), Map("?" -> "Private"))
    val dfrawrpl = dfrawrp.na.replace(Array("occupation"), Map("?" -> "Prof-specialty"))
    val dfrawnona = dfrawrpl.na.replace(Array("native_country"), Map("?" -> "United-States"))

    //converting strings to numeric values
    def indexStringColumns(df:DataFrame, cols:Array[String]):DataFrame = {
      //variable newdf will be updated several times
      var newdf = df
      for(c <- cols) {
        val si = new StringIndexer().setInputCol(c).setOutputCol(c+"-num")
        val sm:StringIndexerModel = si.fit(newdf)
        newdf = sm.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-num", c)
      }
      newdf
    }
    val dfnumeric = indexStringColumns(dfrawnona, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "sex", "native_country", "income"))

    def oneHotEncodeColumns(df:DataFrame, cols:Array[String]):DataFrame = {
      var newdf = df
      for(c <- cols) {
        val onehotenc = new OneHotEncoder().setInputCol(c)
        onehotenc.setOutputCol(c+"-onehot").setDropLast(false)
        newdf = onehotenc.transform(newdf).drop(c)
        newdf = newdf.withColumnRenamed(c+"-onehot", c)
      }
      newdf
    }
    val dfhot = oneHotEncodeColumns(dfnumeric, Array("workclass", "education", "marital_status", "occupation", "relationship", "race", "native_country"))

    val va= new VectorAssembler().setOutputCol("features")*/

//    va.setInputCols(dfhot.columns.diff(Array("income")))
//    val lpoints = va.transform(dfhot).select("features", "income").withColumnRenamed("income", "label")
//
//    //section 8.2.3
//    val splits = lpoints.randomSplit(Array(0.8, 0.2))
//    val adulttrain = splits(0).cache()
//    val adulttest = splits(1).cache()
//
//
//    val lr = new LogisticRegression
//    lr.setRegParam(0.01).setMaxIter(1000).setFitIntercept(true)
//    val lrmodel = lr.fit(adulttrain)
//    val lrmodel = lr.fit(adulttrain, ParamMap(lr.regParam -> 0.01, lr.maxIter -> 500, lr.fitIntercept -> true))
//
//    lrmodel.weights
//    lrmodel.intercept
//
//    //section 8.2.3
//    val testpredicts = lrmodel.transform(adulttest)
//    val bceval = new BinaryClassificationEvaluator()
//    bceval.evaluate(testpredicts)
//    bceval.getMetricName
//
//    bceval.setMetricName("areaUnderPR")
//    bceval.evaluate(testpredicts)
//
//    def computePRCurve(train:DataFrame, test:DataFrame, lrmodel:LogisticRegressionModel) =
//    {
//      for(threshold <- 0 to 10)
//      {
//        var thr = threshold/10.
//        if(threshold == 10)
//          thr -= 0.001
//        lrmodel.setThreshold(thr)
//        val testpredicts = lrmodel.transform(test)
//        val testPredRdd = testpredicts.map(row => (row.getDouble(4), row.getDouble(1)))
//        val bcm = new BinaryClassificationMetrics(testPredRdd)
//        val pr = bcm.pr.collect()(1)
//        println("%.1f: R=%f, P=%f".format(thr, pr._1, pr._2))
//      }
//    }
//    computePRCurve(adulttrain, adulttest, lrmodel)
//    // 0.0: R=1.000000, P=0.238081
//    // 0.1: R=0.963706, P=0.437827
//    // 0.2: R=0.891973, P=0.519135
//    // 0.3: R=0.794620, P=0.592486
//    // 0.4: R=0.694278, P=0.680905
//    // 0.5: R=0.578992, P=0.742200
//    // 0.6: R=0.457728, P=0.807837
//    // 0.7: R=0.324936, P=0.850279
//    // 0.8: R=0.202818, P=0.920543
//    // 0.9: R=0.084543, P=0.965854
//    // 1.0: R=0.019214, P=1.000000
//    def computeROCCurve(train:DataFrame, test:DataFrame, lrmodel:LogisticRegressionModel) =
//    {
//      for(threshold <- 0 to 10)
//      {
//        var thr = threshold/10.
//        if(threshold == 10)
//          thr -= 0.001
//        lrmodel.setThreshold(thr)
//        val testpredicts = lrmodel.transform(test)
//        val testPredRdd = testpredicts.map(row => (row.getDouble(4), row.getDouble(1)))
//        val bcm = new BinaryClassificationMetrics(testPredRdd)
//        val pr = bcm.roc.collect()(1)
//        println("%.1f: FPR=%f, TPR=%f".format(thr, pr._1, pr._2))
//      }
//    }
//    computeROCCurve(adulttrain, adulttest, lrmodel)
//    // 0.0: FPR=1.000000, TPR=1.000000
//    // 0.1: FPR=0.386658, TPR=0.963706
//    // 0.2: FPR=0.258172, TPR=0.891973
//    // 0.3: FPR=0.170781, TPR=0.794620
//    // 0.4: FPR=0.101668, TPR=0.694278
//    // 0.5: FPR=0.062842, TPR=0.578992
//    // 0.6: FPR=0.034023, TPR=0.457728
//    // 0.7: FPR=0.017879, TPR=0.324936
//    // 0.8: FPR=0.005470, TPR=0.202818
//    // 0.9: FPR=0.000934, TPR=0.084543
//    // 1.0: FPR=0.000000, TPR=0.019214
//
//    //section 8.2.5
//    val cv = new CrossValidator().setEstimator(lr).setEvaluator(bceval).setNumFolds(5)
//    val paramGrid = new ParamGridBuilder().addGrid(lr.maxIter, Array(1000)).
//      addGrid(lr.regParam, Array(0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5)).build()
//    cv.setEstimatorParamMaps(paramGrid)
//    val cvmodel = cv.fit(adulttrain)
//    cvmodel.bestModel.asInstanceOf[LogisticRegressionModel].weights
//    cvmodel.bestModel.parent.asInstanceOf[LogisticRegression].getRegParam
//    new BinaryClassificationEvaluator().evaluate(cvmodel.bestModel.transform(adulttest))
//
//    //section 8.2.6
//    val penschema = StructType(Array(
//      StructField("pix1",DoubleType,true),
//      StructField("pix2",DoubleType,true),
//      StructField("pix3",DoubleType,true),
//      StructField("pix4",DoubleType,true),
//      StructField("pix5",DoubleType,true),
//      StructField("pix6",DoubleType,true),
//      StructField("pix7",DoubleType,true),
//      StructField("pix8",DoubleType,true),
//      StructField("pix9",DoubleType,true),
//      StructField("pix10",DoubleType,true),
//      StructField("pix11",DoubleType,true),
//      StructField("pix12",DoubleType,true),
//      StructField("pix13",DoubleType,true),
//      StructField("pix14",DoubleType,true),
//      StructField("pix15",DoubleType,true),
//      StructField("pix16",DoubleType,true),
//      StructField("label",DoubleType,true)
//    ))
//    val pen_raw = sc.textFile("/path/to/penbased.dat", 4).map(x => x.split(", ")).
//      map(row => row.map(x => x.toDouble))
//
//    val dfpen = sqlContext.applySchema(pen_raw.map(Row.fromSeq(_)), penschema)
//    val va = new VectorAssembler().setOutputCol("features")
//    va.setInputCols(dfpen.columns.diff(Array("label")))
//    val penlpoints = va.transform(dfpen).select("features", "label")
//
//    val pensets = penlpoints.randomSplit(Array(0.8, 0.2))
//    val pentrain = pensets(0).cache()
//    val pentest = pensets(1).cache()
//
//    val penlr = new LogisticRegression().setRegParam(0.01)
//    val ovrest = new OneVsRest()
//    ovrest.setClassifier(penlr)
//
//    val ovrestmodel = ovrest.fit(pentrain)
//
//    val penresult = ovrestmodel.transform(pentest)
//    val penPreds = penresult.select("prediction", "label").map(row => (row.getDouble(0), row.getDouble(1)))
//    val penmm = new MulticlassMetrics(penPreds)
//    penmm.precision
//    //0.9018214127054642
//    penmm.precision(3)
//    //0.9026548672566371
//    penmm.recall(3)
//    //0.9855072463768116
//    penmm.fMeasure(3)
//    //0.9422632794457274
//    penmm.confusionMatrix
//    // 228.0  1.0    0.0    0.0    1.0    0.0    1.0    0.0    10.0   1.0
//    // 0.0    167.0  27.0   3.0    0.0    19.0   0.0    0.0    0.0    0.0
//    // 0.0    11.0   217.0  0.0    0.0    0.0    0.0    2.0    0.0    0.0
//    // 0.0    0.0    0.0    204.0  1.0    0.0    0.0    1.0    0.0    1.0
//    // 0.0    0.0    1.0    0.0    231.0  1.0    2.0    0.0    0.0    2.0
//    // 0.0    0.0    1.0    9.0    0.0    153.0  9.0    0.0    9.0    34.0
//    // 0.0    0.0    0.0    0.0    1.0    0.0    213.0  0.0    2.0    0.0
//    // 0.0    14.0   2.0    6.0    3.0    1.0    0.0    199.0  1.0    0.0
//    // 7.0    7.0    0.0    1.0    0.0    4.0    0.0    1.0    195.0  0.0
//    // 1.0    9.0    0.0    3.0    3.0    7.0    0.0    1.0    0.0    223.0
//
//
//    //section 8.3.1
//    val dtsi = new StringIndexer().setInputCol("label").setOutputCol("label-ind")
//    val dtsm:StringIndexerModel = dtsi.fit(penlpoints)
//    val pendtlpoints = dtsm.transform(penlpoints).drop("label").withColumnRenamed("label-ind", "label")
//
//    val pendtsets = pendtlpoints.randomSplit(Array(0.8, 0.2))
//    val pendttrain = pendtsets(0).cache()
//    val pendttest = pendtsets(1).cache()
//
//    val dt = new DecisionTreeClassifier()
//    dt.setMaxDepth(20)
//    val dtmodel = dt.fit(pendttrain)
//
//    dtmodel.rootNode
//    dtmodel.rootNode.asInstanceOf[InternalNode].split.featureIndex
//    //15
//    dtmodel.rootNode.asInstanceOf[InternalNode].split.asInstanceOf[ContinuousSplit].threshold
//    //51
//    dtmodel.rootNode.asInstanceOf[InternalNode].leftChild
//    dtmodel.rootNode.asInstanceOf[InternalNode].rightChild
//
//    val dtpredicts = dtmodel.transform(pendttest)
//    val dtresrdd = dtpredicts.select("prediction", "label").map(row => (row.getDouble(0), row.getDouble(1)))
//    val dtmm = new MulticlassMetrics(dtresrdd)
//    dtmm.precision
//    //0.951442968392121
//    dtmm.confusionMatrix
//    // 192.0  0.0    0.0    9.0    2.0    0.0    2.0    0.0    0.0    0.0
//    // 0.0    225.0  0.0    1.0    0.0    1.0    0.0    0.0    3.0    2.0
//    // 0.0    1.0    217.0  1.0    0.0    1.0    0.0    1.0    1.0    0.0
//    // 9.0    1.0    0.0    205.0  5.0    1.0    3.0    1.0    1.0    0.0
//    // 2.0    0.0    1.0    1.0    221.0  0.0    2.0    3.0    0.0    0.0
//    // 0.0    1.0    0.0    1.0    0.0    201.0  0.0    0.0    0.0    1.0
//    // 2.0    1.0    0.0    2.0    1.0    0.0    207.0  0.0    2.0    3.0
//    // 0.0    0.0    3.0    1.0    1.0    0.0    1.0    213.0  1.0    2.0
//    // 0.0    0.0    0.0    2.0    0.0    2.0    2.0    4.0    198.0  6.0
//    // 0.0    1.0    0.0    0.0    1.0    0.0    3.0    3.0    4.0    198.0
//
//    //section 8.3.2
//    val rf = new RandomForestClassifier()
//    rf.setMaxDepth(20)
//    val rfmodel = rf.fit(pendttrain)
//    rfmodel.trees
//    val rfpredicts = rfmodel.transform(pendttest)
//    val rfresrdd = rfpredicts.select("prediction", "label").map(row => (row.getDouble(0), row.getDouble(1)))
//    val rfmm = new MulticlassMetrics(rfresrdd)
//    rfmm.precision
//    //0.9894640403114979
//    rfmm.confusionMatrix
//    // 205.0  0.0    0.0    0.0    0.0    0.0    0.0    0.0    0.0    0.0
//    // 0.0    231.0  0.0    0.0    0.0    0.0    0.0    0.0    1.0    0.0
//    // 0.0    0.0    221.0  1.0    0.0    0.0    0.0    0.0    0.0    0.0
//    // 5.0    0.0    0.0    219.0  0.0    0.0    2.0    0.0    0.0    0.0
//    // 0.0    0.0    0.0    0.0    230.0  0.0    0.0    0.0    0.0    0.0
//    // 0.0    1.0    0.0    0.0    0.0    203.0  0.0    0.0    0.0    0.0
//    // 1.0    0.0    0.0    1.0    0.0    0.0    216.0  0.0    0.0    0.0
//    // 0.0    0.0    1.0    0.0    2.0    0.0    0.0    219.0  0.0    0.0
//    // 0.0    0.0    0.0    1.0    0.0    0.0    0.0    1.0    212.0  0.0
//    // 0.0    0.0    0.0    0.0    0.0    0.0    2.0    2.0    2.0    204.0
//
//
//    //section 8.4.1
//    val penflrdd = penlpoints.map(row => (row(0).asInstanceOf[Vector], row(1).asInstanceOf[Double]))
//    val penrdd = penflrdd.map(x => x._1).cache()
//    val kmmodel = KMeans.train(penrdd, 10, 5000, 20)
//
//    kmmodel.computeCost(penrdd)
//    //4.930151488324314E7
//    math.sqrt(kmmodel.computeCost(penrdd)/penrdd.count())
//    //66.97176923983041
//
//    val kmpredicts = penflrdd.map(feat_lbl => (kmmodel.predict(feat_lbl._1).toDouble, feat_lbl._2))
//
//    //rdd contains tuples (prediction, label)
//    def printContingency(rdd:RDD[(Double, Double)], labels:Seq[Int])
//    {
//      val numl = labels.size
//      val tablew = 6*numl + 10
//      var divider = "".padTo(10, '-')
//      for(l <- labels)
//        divider += "+-----"
//
//      var sum:Long = 0
//      print("orig.class")
//      for(l <- labels)
//        print("|Pred"+l)
//      println
//      println(divider)
//      val labelMap = scala.collection.mutable.Map[Double, (Double, Long)]()
//      for(l <- labels)
//      {
//        //filtering by predicted labels
//        val predCounts = rdd.filter(p => p._2 == l).countByKey().toList
//        //get the cluster with most elements
//        val topLabelCount = predCounts.sortBy{-_._2}.apply(0)
//        //if there are two (or more) clusters for the same label
//        if(labelMap.contains(topLabelCount._1))
//        {
//          //and the other cluster has fewer elements, replace it
//          if(labelMap(topLabelCount._1)._2 < topLabelCount._2)
//          {
//            sum -= labelMap(l)._2
//            labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
//            sum += topLabelCount._2
//          }
//          //else leave the previous cluster in
//        }
//        else
//        {
//          labelMap += (topLabelCount._1 -> (l, topLabelCount._2))
//          sum += topLabelCount._2
//        }
//        val predictions = predCounts.sortBy{_._1}.iterator
//        var predcount = predictions.next()
//        print("%6d".format(l)+"    ")
//        for(predl <- labels)
//        {
//          if(predcount._1 == predl)
//          {
//            print("|%5d".format(predcount._2))
//            if(predictions.hasNext)
//              predcount = predictions.next()
//          }
//          else
//            print("|    0")
//        }
//        println
//        println(divider)
//      }
//      println("Purity: "+sum.toDouble/rdd.count())
//      println("Predicted->original label map: "+labelMap.mapValues(x => x._1))
//    }
//    printContingency(kmpredicts, 0 to 9)
//    // orig.class|Pred0|Pred1|Pred2|Pred3|Pred4|Pred5|Pred6|Pred7|Pred8|Pred9
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      0    |    1|  379|   14|    7|    2|  713|    0|    0|   25|    2
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      1    |  333|    0|    9|    1|  642|    0|   88|    0|    0|   70
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      2    | 1130|    0|    0|    0|   14|    0|    0|    0|    0|    0
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      3    |    1|    0|    0|    1|   24|    0| 1027|    0|    0|    2
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      4    |    1|    0|   51| 1046|   13|    0|    1|    0|    0|   32
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      5    |    0|    0|    6|    0|    0|    0|  235|  624|    3|  187
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      6    |    0|    0| 1052|    3|    0|    0|    0|    1|    0|    0
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      7    |  903|    0|    1|    1|  154|    0|   78|    4|    1|    0
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      8    |   32|  433|    6|    0|    0|   16|  106|   22|  436|    4
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    //      9    |    9|    0|    1|   88|   82|   11|  199|    0|    1|  664
//    // ----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----
//    // Purity: 0.6672125181950509
//    // Predicted->original label map: Map(8.0 -> 8.0, 2.0 -> 6.0, 5.0 -> 0.0, 4.0 -> 1.0, 7.0 -> 5.0, 9.0 -> 9.0, 3.0 -> 4.0, 6.0 -> 3.0, 0.0 -> 2.0)
//
//
//
//
//
//    def findK(rdd:RDD[Vector], ks:Seq[Int])
//    {
//      for(k <- ks)
//      {
//        val model = KMeans.train(rdd, k, 500, 10)
//        println("K=%d: %f".format(k, model.computeCost(rdd)))
//      }
//    }
//    findK(pentrainrdd, pentestrdd, 2 to 20)



  }

}
