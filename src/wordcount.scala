import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordcount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "c:\\hadoop\\")
  val conf = new SparkConf().setAppName("wordcount").setMaster("local")
 val sc = new SparkContext(conf)
  val input = sc.textFile("file2///")
  val words = input.flatMap(_.split(" "))
  val wordlower = words.map(_.toLowerCase())
  val wordMap = wordlower.map((_,1))
  val finalcount = wordMap.reduceByKey(_+_)
 
  val finalresult = finalcount.sortBy(x =>x._2)
  val results = finalresult.collect
  for(result <- results){
    val word = result._1
    val count = result._2
    println(s"$word:$count")
  }
  scala.io.StdIn.readLine()
}