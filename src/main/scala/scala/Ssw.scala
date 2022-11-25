package scala
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
object Ssw extends App{
  val conf = new SparkConf()
    .setAppName("ReadParquet")
    .set("spark.driver.bindAddress", "127.0.0.1")
    .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
    .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .set("spark.driver.memory", "4g")
    .set("spark.executor.memory", "8g")


  val spark = SparkSession.builder()
    .master("local[*]")
    .config(conf)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  import org.apache.spark.sql.functions._
  val componentDF = spark.read.parquet("/home/prasoon/Desktop/cosdata/dsw/dsw/component").withColumnRenamed("CTRYCODE", "COUNTRY_CODE")


  val headerDF = spark.read.parquet("/home/prasoon/Desktop/cosdata/dsw/dsw/header")
  val quoteDF = spark.read.parquet("/home/prasoon/Desktop/cosdata/dsw/dsw/quote")
//    componentDF.show()
//    headerDF.show()
//    quoteDF.show()
  println(componentDF.count())
  println(headerDF.count())
  println(quoteDF.count())
  componentDF.select("COUNTRY_CODE").show()
  val newdf =  quoteDF.join(componentDF, quoteDF("COUNTRY_CODE")=== componentDF("COUNTRY_CODE"))
  //.join(headerDF, Seq("QUOTE_ID")

  newdf.show()


}