import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object sparksql extends App{
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
  .option("path","C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\orderss.csv")
  .load
  
  orderDf.createOrReplaceTempView("orders")
  //val resultDf = spark.sql("select order_status, count(*) as status_count from orders group by order_status order by status_count desc")
  
  val resultDf = spark.sql("select order_customer_id, count(*) as total_orders from orders where order_status= 'CLOSED' group by order_customer_id order by total_orders desc")
  
  
  resultDf.show
  scala.io.StdIn.readLine()
  spark.stop()
  
  
}