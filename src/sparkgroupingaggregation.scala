import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object sparkgroupingaggregation extends App {
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
    
  
  //using column expression 
 val summaryDf =  invoiceDf.groupBy("country", "InvoiceNo")
  .agg(sum("Quantity").as("TotalQuantity"),
      sum(expr("Quantity * UnitPrice")).as("InvoiceValue")
      )
      summaryDf.show
  
      //using string expression
     val summaryDf1=  invoiceDf.groupBy("country", "InvoiceNo")
      .agg(expr("sum(Quantity) as TotalQuantity"),
          expr("sum(Quantity * UnitPrice) as InvoiceValue")
          )
          
          summaryDf1.show
          
          
          //using sql expression
        invoiceDf.createOrReplaceTempView("sales")
        
       val summaryDf2 =  spark.sql("""select Country, InvoiceNo,
            sum(Quantity) as TotalQuantity,
             sum(Quantity * UnitPrice)  as InvoiceValue from 
             sales group by Country, InvoiceNo""")
            
             summaryDf2.show()
          
          
          
  spark.stop()
  
  
}