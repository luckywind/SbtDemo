package com.coupang.buybox

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xcheng on 4/7/16.
  */
object LogisticRegression {
  def main(args: Array[String]) {
   val conf: SparkConf = new SparkConf().setAppName("buybox LR").setMaster("local[4]")
   val sparkContext: SparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("OFF")
   val sqlContext: SQLContext = new SQLContext(sparkContext)

/*
     get data
 */
    val OrderDataPATH: String = "/Users/xcheng/Desktop/buybox/Order.csv"
    val cancellDataPATH  ="/Users/xcheng/Desktop/buybox/Cancellation_order.csv"
    val vendor_itemsPATH="/Users/xcheng/Desktop/buybox/vendor_items.csv"
    val DELIVERY_STATUS_TRACEPATH="/Users/xcheng/Desktop/buybox/DELIVERY_STATUS_TRACE.csv"
    val priceHistoryPATH="/Users/xcheng/Desktop/buybox/priceHistory.csv"


    val rawOrder: RDD[String] = sparkContext.textFile(OrderDataPATH)
    val rawCancell = sparkContext.textFile(cancellDataPATH)
    val rawVendor_items: RDD[String] = sparkContext.textFile(vendor_itemsPATH)
    val rawDELIVERY_STATUS_TRACE=sparkContext.textFile(DELIVERY_STATUS_TRACEPATH)
    val rawpriceHistory: RDD[String] = sparkContext.textFile(priceHistoryPATH)
/*
 create table
 */
    createCancellTable(sqlContext, rawCancell)
    createOrderTable(sqlContext,rawOrder)
    sqlContext.createDataFrame(rawVendor_items.filter(!isCollumnLine(_,"VENDORID")).filter(line=>line.split(",").length>3).map(line=>vendor_itemsParser(line)))
      .toDF("VENDORITEMID","ITEMID","VENDORID","CREATEDAT").registerTempTable("Vendor_itemstable")
    sqlContext.createDataFrame(rawDELIVERY_STATUS_TRACE.filter(!isCollumnLine(_,"DELIVERYSRL")).map(line=>DELIVERY_STATUS_TRACEParser(line)))
      .toDF("DELIVERYSRL","DELIVERYUSEDTIME").registerTempTable("DELIVERY_STATUS_TRACEtable")
    sqlContext.createDataFrame(rawpriceHistory.filter(!isCollumnLine(_,"SALEPRICE")).map(line=>priceHistoryParser(line)))
      .toDF("VENDORITEMID","SALEPRICE").registerTempTable("priceHistorytable")

    /*
    test
     */
    sqlContext.sql("select count(*) from OrderTable ").show()
    sqlContext.sql("select count(*)  from Vendor_itemstable limit 3").show()
    sqlContext.sql("select count(*)  from DELIVERY_STATUS_TRACEtable limit 3").show()
    sqlContext.sql("select count(*)  from priceHistorytable limit 3").show()
    /*
    +------------+--------+---------+----+---------+----------+--------+-------+------------+------------+
|DELIVERY_SRL|SALE_DAY|  ORDERID|KIND|SALE_TYPE|SALE_PRICE|SALE_AMT| ITEMID|SHIPPINGCOST|VENDORITEMID|
+------------+--------+---------+----+---------+----------+--------+-------+------------+------------+
|   213366621|20160120|527136230|   1|        S|    7920.0|       1|6232853|         0.0|  3008071880|
|           0|20160120|525922174|   2|        S|  -65000.0|      -1| 406070|         0.0|  3000194511|Order
|   213124849|20160120|526814649|   1|        S|    2500.0|       1|4932747|         0.0|  3006085469|
+------------+--------+---------+----+---------+----------+--------+-------+------------+------------+

+------------+--------+---------+
|VENDORITEMID|  ITEMID| VENDORID|
+------------+--------+---------+
|  3018735455|12284780|A00025027|venderitem
|  3018735695|12284990|A00060102|
|  3018735935|12285054|A00060102|
+------------+--------+---------+

+-----------+----------------+
|DELIVERYSRL|DELIVERYUSEDTIME|
+-----------+----------------+
|  161852279|             221|delivery_status
|  163250780|             126|
|  164450558|              80|
+-----------+----------------+

+------------+---------+
|VENDORITEMID|SALEPRICE|
+------------+---------+
|  3000268720|  16000.0|priceHistory
|  3000230851|   2300.0|
|  3000169024|  11690.0|
+------------+---------+
     */
    //所有
//     sqlContext.sql("select ORDERID, SALE_PRICE,DELIVERYUSEDTIME from DELIVERY_STATUS_TRACEtable d  join  OrderTable o on " +
//       "o.DELIVERY_SRL=d.DELIVERYSRL join Vendor_itemstable v on o.VENDORITEMID=v.VENDORITEMID").show()
  /*
    dataport
    select ORDERID, SALE_PRICE,DELIVERYUSEDTIME from DELIVERY_STATUS_TRACE d  join  MART_SALE_DAILY o on
       o.DELIVERY_SRL=d.DELIVERYSRL join Vendor_items v on o.VENDORITEMID=v.VENDORITEMID


       */






  }

  def createCancellTable(sqlContext: SQLContext, rawCancell: RDD[String]): Unit = {
    sqlContext.createDataFrame(rawCancell.filter(!isCollumnLine(_, "ORDERID")).filter(line => line.split(",").length == 32).map(line => cancellParser(line)))
      .toDF("CANCELTYPE", "ORDERID", "REFUNDDELIVERYDUTY").registerTempTable("cancellTable")
  }

  def createOrderTable(sqlContext: SQLContext, rawOrder: RDD[String]): Unit = {
    sqlContext.createDataFrame(rawOrder.filter(!isCollumnLine(_, "ORDERID")).filter(line => line.split(",").length == 100).map(line => orderParser(line))
    ).toDF("DELIVERY_SRL","SALE_DAY", "ORDERID", "KIND", "SALE_TYPE", "SALE_PRICE", "SALE_AMT", "ITEMID", "SHIPPINGCOST", "VENDORITEMID").registerTempTable("OrderTable")
    sqlContext.sql("select * from OrderTable")
  }

  def isCollumnLine(line:String, collumnstr:String):Boolean={
    if(line!=null && line.contains(collumnstr))true
    else false
  }


  def orderParser(line:String): (Int,Int, Int, Int, String, Double, Int, Int, Double, Long) = {
    val splited = line.replace("\"", "").split(",")
    val SALE_DAY =if(splited(0) =="")0 else splited(0).toInt
    val KIND =if(splited(2) =="")0 else splited(2).toInt
    val SALE_TYPE = splited(3)
    val ORDERID =if(splited(5) =="")0 else splited(5).toInt
    val SALE_PRICE =if(splited(6) =="")0 else splited(6).toDouble
    val SALE_AMT =if(splited(7) =="")0 else splited(7).toInt
    val ITEMID = if(splited(89) =="")0 else splited(89).toInt
    val SHIPPINGCOST = if(splited(72) =="")0 else splited(72).toDouble
    val DELIVERY_SRL = if(splited(75)=="")0 else splited(75).toInt
    val VENDORITEMID = if (splited(90) == "") 0 else splited(90).toLong //toLong
    (DELIVERY_SRL,SALE_DAY, ORDERID, KIND, SALE_TYPE, SALE_PRICE, SALE_AMT, ITEMID, SHIPPINGCOST, VENDORITEMID)
  }


  def cancellParser(line:String)={
    val splited = line.replace("\"","").split(",")
    val CANCELTYPE = line(1).toString
    val ORDERID = if(splited(3) =="")0 else line(3).toInt
    val REFUNDDELIVERYDUTY=line(15).toString
    (CANCELTYPE,ORDERID,REFUNDDELIVERYDUTY)

  }

  def vendor_itemsParser(line:String)={
    val splited = line.replace("\"","").split(",")
    val VENDORITEMID =splited(0)
    val ITEMID = splited(1).toInt
    val VENDORID=splited(2)
    val CREATEDAT=splited(14)
    (VENDORITEMID,ITEMID,VENDORID,CREATEDAT)
  }
  def DELIVERY_STATUS_TRACEParser(line:String)={
    val splited: Array[String] = line.replace("\"","").split(",")
    val DELIVERYSRL =splited(0)
    val DELIVERYUSEDTIME=splited(10)
    (DELIVERYSRL,DELIVERYUSEDTIME)
  }
  def priceHistoryParser(line:String)={
    val splited: Array[String] = line.replace("\"","").split(",")
    val VENDORITEMID=splited(1)
    val SALEPRICE=splited(2).toDouble
    (VENDORITEMID,SALEPRICE)

  }


}
//    print(rawOrder.count())  //9381001
//
//    createOrderTable(sqlContext, rawOrder)//.show()
//
//    sqlContext.sql("select count(*) from OrderTable").show() //9116051
//
//    sqlContext.sql("select count(*) from OrderTable group by KIND").show()//8651234|| 464817
//
//    sqlContext.sql("select count(*) from OrderTable group by SALE_TYPE").show()//1936082||7179969
//
//
//    sqlContext.sql("select count(*) from OrderTable where SHIPPINGCOST>0").show()//162189
//
//
//    sqlContext.sql("select count(*) from OrderTable group by SALE_AMT").show()//351952||1936083||6827834||     20||    162

/**
  *+--------+---------+----+---------+----------+--------+-------+------------+------------+
  *|SALE_DAY|  ORDERID|KIND|SALE_TYPE|SALE_PRICE|SALE_AMT| ITEMID|SHIPPINGCOST|VENDORITEMID|
  *+--------+---------+----+---------+----------+--------+-------+------------+------------+
  *|20160120|527136230|   1|        S|    7920.0|       1|6232853|         0.0|  3008071880|
  *|20160120|525922174|   2|        S|  -65000.0|      -1| 406070|         0.0|  3000194511|
  *|20160120|526814649|   1|        S|    2500.0|       1|4932747|         0.0|  3006085469|
  *|20160120|527037629|   1|        S|   12000.0|       1| 181280|         0.0|  3000126183|
  *|20160120|526868547|   1|        S|    7000.0|       1| 196670|         0.0|  3000124553|
  *|20160120|527088664|   1|        S|   11950.0|       1| 177576|         0.0|  3000103007|
  *|20160120|526817817|   1|        S|    3000.0|       1| 344450|         0.0|  3000252126|
  *|20160120|527248414|   1|        S|   22600.0|       1|5239593|         0.0|  3006540583|
  *|20160120|527219676|   1|        S|   39900.0|       1|4429409|         0.0|  3005380585|
  *|20160120|527260909|   1|        D|       0.0|       0|1963930|         0.0|  3001924252|
  */