import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object sparkcolumnreferance extends App{
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  val orderDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSehema",true)
  .option("path","C:\\Users\\juli\\workspace\\sparkpractice\\file2\\orderss.csv")
  .load
  
  orderDf.select("order_id","order_status").show
 
  import spark.implicits._
  
  orderDf.select($"order_id",$"order_date",$"order_customer_id", 'order_status).show(false)
   //orderDf.select("order_id","concat(order_status,'_STATUS')").show  //can't mix column string and expression
  orderDf.selectExpr("order_id","concat(order_status,'_STATUS')").show(false)
 
  
  scala.io.StdIn.readLine()
  spark.stop()
}