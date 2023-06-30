




import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object friendsbyage extends App {
  def parseline(line: String)={
    val field = line.split(",")
    val age = field(2).toInt
    val numfriends = field(3).toInt
    (age,numfriends)
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "c:\\hadoop\\")
  val conf = new SparkConf().setAppName("wordcount").setMaster("local")
  val sc = new SparkContext(conf)
  val input = sc.textFile("file///friends-data.csv")
  val mappedinput = input.map(parseline)
 
  // val mappedfinal = mappedinput.map(x => (x._1,(x._2,1)))
  
  val mappedfinal = mappedinput.mapValues(x => (x,1))
  val totalbyage = mappedfinal.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
 
  //val averagebyage = totalbyage.map(x => (x._1,x._2._1/x._2._2)).sortBy(x => x._2) 
 
  val averagebyage = totalbyage.mapValues(x => x._1/x._2).sortBy(x => x._2)
  averagebyage.collect.foreach(println)
      
 
}