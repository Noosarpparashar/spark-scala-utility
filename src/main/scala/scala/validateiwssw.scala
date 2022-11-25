package scala
import org.apache.spark
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}



object validateiwssw extends App{
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


  val file_location = "/FileStore/tables"
  val  file_type = "parquet"
  val infer_schema = "false"
  val first_row_is_header = "true"
  val delimiter = ","

  val df= spark.read.option("header", "true").csv("/home/prasoon/Downloads/IW_SSW_Data.csv").limit(300000)
  df.show()
  println(df.select("CURRENCYCODE").distinct().show())
  println(df.select("CURRENCYCODE").distinct().count())

/*\
JPY|
|         EUR|
|
|         INR|
|         TWD|
|
|
|         KRW|
|          KW|
|
|         THB|
|         CHF|
|         ZAR|
|         SGD|
|         HKD|
 */

}
