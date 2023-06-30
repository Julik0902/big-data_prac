import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

object sparkJoin extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  //reading order data
  val orderDf = spark.read
  .format("json")
  //.option("inferSchema",true)
  //.option("header",true)
  .option("path","C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\orders-join.json")
  .load()
  
  //rename the column 
  //val orderNew = orderDf.withColumnRenamed("order_customer_id", "cust_id")
  
  
  //reading customer data
  val customerDf = spark.read
  .format("json")
  //.option("inferSchema",true)
  //.option("header",true)
  .option("path","C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\customers.json")
  .load()
  
  
  
  spark.sql("SET spark.sql.outoBroadcastJoinThreshold = -1")  // this will do simple join not broadcast join in inner type join
  //join condition
    //val joincondition = orderNew.col("cust_id") === customerDf.col("customer_id")
  
  
  val joincondition = orderDf.col("order_customer_id") === customerDf.col("customer_id")
  
    //type of join
    val joinType = "inner" 
  
      orderDf.join(broadcast(customerDf, joincondition, joinType))
      //.drop(orderDf.col("order_customer_id"))
      //.select("order_id", "customer_id", "customer_fname")
      //.sort("order_id")
      //.withColumn("order_id",expr("coalesce(order_id, -1)"))  //with deal with  null vale we use coalesce 
      .show
  
  
  //customerDf.show
  
  
   scala.io.StdIn.readLine()
  spark.stop()
}