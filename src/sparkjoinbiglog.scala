
 import org.apache.log4j.Level
 import org.apache.log4j.Logger
 import org.apache.spark.sql.SparkSession

object sparkjoinbiglog  {
  

   case class Logging(Level:String, datetime:String)
   
   def mapper(line:String): Logging = {
     val fields = line.split(",")
     val logging:Logging = Logging(fields(0), fields(1))
     return logging
   }
   
   //our main function where the action happen
   def main(args: Array[String]){
   
  Logger.getLogger("org").setLevel(Level.ERROR)
 
  val spark = SparkSession
  .builder
  .appName("SparkSQL")
  .master("local[*]")
  .getOrCreate()
  
  /*val mylist = List("WARN,2016-12-31 04:19:32", 
      "FATAL,2016-12-31 03:22:34",
      "WARN,2015-4-21 14:32:21",
      "FATAL,2015-4-21 19:23:20")
      
   val rdd1 = spark.sparkContext.parallelize(mylist)
      val rdd2 = rdd1.map(mapper)
      val df1 = rdd2.toDF()
      df1.show()
      
      df1.createOrReplaceTempView("logging_table")
      //spark.sql("select * from logging_table").show
  //spark.sql("select level,count(datetime) from logging_table group by level order by level")
  //.show()
  
  val df2 = spark.sql("select level,date_format(datetime,'MMM') as month  from logging_table").show
  df2.createOrReplaceTempView("new_logging_table")
  spark.sql("select level, month, count(1) from new_logging_table group by level, month").show
  */
  val df3 = spark.read
  .option("header", true)
  .csv("C:\\Users\\syoge\\workspace\\sparkpractice\\file2\\biglog.txt")
    
  df3.createOrReplaceTempView("my_new_logging_table")
   val results = spark.sql("select level, date_format(datetime, 'MMM')as month, count(1) as total from my_new_logging_table group by level, month").show
  
   
   
   
     
  val columns = List("January", "February", "March", "April", "May", "June","July",  "August", "September", "October", "November", "December")
  
  
  //val result1 = spark.sql("select level, date_format(datetime, 'MMM')as month, cast(first(date_format(datetime, 'M'))as int) as mothnum,count(1) as total from my_new_logging_table group by level, month order by monthnum, level")
  //val result1 = spark.sql("select level, date_format(datetime, 'MMM')as month, cast(date_format(datetime, 'M'))as int) as mothnum from my_new_logging_table ").groupBy("level").pivot("monthnum").count().show(100)
  val result1 = spark.sql("select level, date_format(datetime, 'MMM')as month from my_new_logging_table ").groupBy("level").pivot("month", columns).count().show(100)
  
  //val results2 = result1.drop("monthnum")
  
 // results2.show
  
  
  List("january", "febuary", "march", "april", "may", "june", "august", "september", "october", "november", "december") 
  
  
  
  
  
  
  
  
  
  
  
  
  // scala.io.StdIn.readLine()
  
  
  spark.stop()
}
}