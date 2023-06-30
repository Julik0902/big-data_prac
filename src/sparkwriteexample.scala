import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode


object sparkwriteexample extends App{
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()   //for hive enable
  .getOrCreate()
  
  val orderDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSehema",true)
  .option("path","C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\orderss.csv")
  .load
  
  
  //print("orderDf has "+orderDf.rdd.getNumPartitions)
   //val orderRep = orderDf.repartition(2)
   //print("orderRep has "+ orderRep.rdd.getNumPartitions)
   
   //when we want as output as folder
   
  /*orderRep.write
  .format("csv")
  .partitionBy("order_status")
  .mode(SaveMode.Overwrite)
  .option("maxRecordsPerFile",2000)
  .option("path","C:\\Users\\syoge\\workspace\\sparkpractice\\newfolder1")
  .save()*/
  
  spark.sql("create database if not exists retail")
  
  //when we save as table of output
  
  orderDf.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .bucketBy(4,"order_customer_id")
  .sortBy("order_customer_id")
  .saveAsTable("retail.orders")
  
  spark.catalog.listTables("retail").show()
  
  
  scala.io.StdIn.readLine()
  spark.stop()
}