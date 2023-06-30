

import java.sql.Timestamp


import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


case class OrdersData(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_staus: String)

object dataframeexample extends App {
  //Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  val orderDf: Dataset[Row] = spark.read
  .option("header",true)
  .option("inferSehema",true)
  .csv("C:\\courses\\trendytech_bigdata\\workspace\\sparkpractice\\file2\\orderss.csv")
  
 
  import spark.implicits._
  
  val ordersDs = orderDf.as[OrdersData]
  ordersDs.filter(x => x.order_id <10)
  scala.io.StdIn.readLine()
  
  
  //processing
  spark.stop()
}