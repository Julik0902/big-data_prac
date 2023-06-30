import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object sparkaggregations extends App  {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val invoiceDf = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema","true")
  .option("path","C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\order_data.csv")
  .load()
  
  
  invoiceDf.select(
      count("*").as("RowCount"),
      sum("Quantity").as("TotalQuantity"),
      avg("UnitPrice").as("AvgPrice"),
      countDistinct("InvoiceNo").as("CountDistinct")
      ).show()
   
      //string using 
      invoiceDf.selectExpr(
      "count(*) as RowCount",
      "sum(quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgPrice",
      "count(Distinct(InVoiceNo)) as CountDistinct"
      ).show
  
      invoiceDf.createOrReplaceTempView("sales")
  
  spark.sql("select count(*), sum(Quantity), avg(UnitPrice), count(distinct(InvoiceNo)) from sales").show 
  
  spark.stop()
}