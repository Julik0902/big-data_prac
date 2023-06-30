

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object totalspent extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "c:\\hadoop\\")
  val conf = new SparkConf().setAppName("totalspent").setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile("file///customerorders.csv")
  val mappedinput = input.map(x => (x.split(",")(0),x.split("'")(2).toFloat))
  val totalbycustomer = mappedinput.reduceByKey((x,y) => x+y)
  val sortedtotal = totalbycustomer.sortBy(x => x._2)
  val results = sortedtotal.collect
  results.foreach(println)
  
}