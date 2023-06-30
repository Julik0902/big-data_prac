import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object sparkwindowaggregation extends App {
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
  .option("path","C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\windowdata.csv")
  .load()
  
  val mywindow = Window.partitionBy("country")
  .orderBy("weeknum")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
 val myDf =  invoiceDf.withColumn("RunningTotal", 
              sum("invoicevalue").over(mywindow))
   myDf.show()
  
  spark.stop()
}