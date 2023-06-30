import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object moiveRating extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val orderDf = spark.read
    .format("csv")
    .option("header", true)
    //.option("inferSehema",true)
    .option("path", "C:\\courses\\trendytech_bigdata\\workspace\\sparkpractice\\file2\\orderss.csv")
    //.csv("C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\orderss.csv")
    .load
  //val groupedOrdersDf = orderDf
  //.repartition(4)
  //.where("order_customer_id > 10000")
  //.select("order_id","order_customer_id")
  //.groupBy("order_customer_id")
  //.count()
  //groupedOrdersDf.foreach(x => {
  //println(x)
  //})

  orderDf.printSchema()
  orderDf.show()
  //groupedOrdersDf.show()
  //orderDf.printSchema()
  //Logger.getLogger(getClass.getName).info("my application is completed successfully")

  scala.io.StdIn.readLine()

  //processing
  spark.stop()
}