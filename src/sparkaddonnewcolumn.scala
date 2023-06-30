import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr


case class person(name:String, age:Int, city:String)

object sparkaddonnewcolumn extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
 def ageCheck(age:Int) ={
    if (age > 18) "Y" else "N"
  }
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  //reading  a file using dataframe reader api
  val df = spark.read
  .format("csv")
  .option("inferSchema",true)
  .option("path","C:\\Users\\juli\\workspace\\sparkpractice\\file2\\dataset1")
  .load()
  
   //column object expression ud
  /*import spark.implicits._
  val df1: Dataset[Row]=df.toDF("name","age","city")
  val parseAgeFunction= udf(ageCheck(_:Int):String)
  
  val df2 = df1.withColumn("adult",parseAgeFunction($"age"))
  df2.show
  
  spark.catalog.listFunctions().filter(x=> x.name == "parseAgeFunction").show
  //df.printSchema()
  //df1.show*/
 //val ds1 =  df1.as[person]  covert df to ds 
  //val df2 = ds1.toDF().as[person]     covert to ds to df
  
  
  val df1: Dataset[Row]=df.toDF("name","age","city")
  
   spark.udf.register("parseAgeFunction",ageCheck(_:Int):String)
   //spark.udf.register("parseAgeFunction",(x:Int) => {if (x>18) "Y" else "N"})
   val df2 =  df1.withColumn("adult_value", expr("parseAgeFunction(age)"))
  
   df2.show
 
    spark.catalog.listFunctions().filter(x=> x.name == "parseAgeFunction").show
    df1.createOrReplaceTempView("peopletable")
 
    spark.sql("select name. age, city, parseAgeFunction(age) as adult from peopletable").show
 
    scala.io.StdIn.readLine()
  
  
  
  spark.stop()
}