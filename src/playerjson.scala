import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import java.sql.Timestamp

object playerjson extends App {
  case class orders(order_id: Int, order_date: Timestamp, customer_id: Int, order_status: String)
  //Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  /*val ordersSchema = StructType(List(
StructField("orderid", IntegerType),
StructField("orderdate", TimestampType),
StructField("customerid", IntegerType),
StructField("status", StringType)
))*/

  /*val orderDf = spark.read
   .format("json")
   .option("path","C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\players.json")
   .option("mode","DROPMALFORMED")
   .load*/

  val ordersSchemaDDL = "orderid Int, orderdate String,custid Int, ordstatus String"

  val orderDf = spark.read
    .format("csv")
    .option("header", true)
    .schema(ordersSchemaDDL)
    .option("path", "C:\\Users\\juli\\workspace\\sparkpractice\\file2\\orderss.csv")
    .load

  import spark.implicits._

  val orderDs = orderDf.as[orders]

  orderDf.printSchema
  orderDf.show()

  scala.io.StdIn.readLine()
  spark.stop()

}