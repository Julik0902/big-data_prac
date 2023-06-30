import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object sparkUnstructureData extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val myregex = """^(\S+) (\S+)\t(\S+),(\S+)""".r
  
  case class Orders(order_id: Int, customer_id: Int, order_status:String)
  
 def parser(line: String) = {
    line match{
      case myregex(order_id, date, customer_id, order_status) =>
        Orders(order_id.toInt,customer_id.toInt,order_status)
    }
  }
  
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val lines = spark.sparkContext.textFile("C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\orders.csv")
   
  
  import spark.implicits._
  
  val orderDS =lines.map(parser).toDS().cache()
  
  orderDS.printSchema()
  orderDS.select("order_id").show()
  orderDS.groupBy("order_status").count().show()
 
  
  scala.io.StdIn.readLine()
  spark.stop()
}