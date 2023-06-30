
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ratingcalculator extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "c:\\hadoop\\")
  val conf = new SparkConf().setAppName("wordcount").setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile("file///movie-data.data")
  val mappedinput = input.map(x => x.split("\t")(2))
  val results = mappedinput.countByValue
 // val rating = mappedinput.map(x => (x,1))
 // val reducerating = rating.reduceByKey((x,y) => x+y)
  //val results = reducerating.collect
  results.foreach(println)
}