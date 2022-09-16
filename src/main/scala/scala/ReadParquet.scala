package scala

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}


object ReadParquet extends App{

  val conf = new SparkConf()
    .setAppName("ReadParquet")
  .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
  .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
  .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
  .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

  val spark = SparkSession.builder()
    .master("local[*]")
    .config(conf)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  import org.apache.spark.sql.functions._
//  val parquetFileDF = spark.read.parquet("/home/prasoon/Downloads/Archive/CTMP_EP01_TX_BRAND")
//  parquetFileDF.show()
//  println(parquetFileDF.schema)
 // parquetFileDF.select(parquetFileDF("FLAG3")).distinct.show()
// val dfWithLength= parquetFileDF.withColumn("col_length",length($"LASTUPDUSERTYPE")).cache()
//
//  val Row(maxValue: Int) = dfWithLength.agg(max("col_length")).head()
//  println(maxValue)
//
//  //dfWithLength.filter($"col_length" === maxValue).show()
//
//  //CheckUNique
//  println(parquetFileDF.select("CTMPPGRID").distinct().count() == parquetFileDF.count())
//

val df = spark.read.csv("/home/prasoon/Downloads/")

}
