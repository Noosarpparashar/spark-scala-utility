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
  val componentDF = spark.read.parquet("/home/prasoon/Downloads/dsw1/dsw/component").limit(800000)
    .withColumnRenamed("CTRYCODE","COUNTRY_CODE")
    .withColumn("QUOTE_ID",trim(col("QUOTE_ID")))
    .withColumn("COUNTRY_CODE",trim(col("COUNTRY_CODE")))
  //componentDF.filter(col("QUOTE_ID").isin("4150292", "4160672", "4203447" ,"4302343", "4302360")).show()
  //componentDF.filter(col("QUOTE_ID").isin("4181382")).show()
  val aaa = Seq("PRODUCT_OFFERING_SKEY", "GEO_CTRY_SKEY")
val aa1 = aaa.map(x=> (x.concat("u")))
  val abc = componentDF.select(aaa.map(m=>col(m)):_*)
    .toDF(aa1:_*)
  abc.select(aa1.map(m=>col(m)):_*).toDF(aaa:_*).show()



//  val parquetFileDF = spark.read.parquet("/home/prasoon/Downloads/EPRICER_HEADER_FACT/EPRICER_HEADER_FACT/transform.parquet")
//  parquetFileDF.show()
//  println(parquetFileDF.schema)
//  parquetFileDF.select(parquetFileDF("FLAG3")).distinct.show()
// val dfWithLength= parquetFileDF.withColumn("col_length",length($"LASTUPDUSERTYPE")).cache()
//
//  val Row(maxValue: Int) = dfWithLength.agg(max("col_length")).head()
//  println(maxValue)
//
//  //dfWithLength.filter($"col_length" === maxValue).show()
//
//  //CheckUNique
//  println(parquetFileDF.select("CTMPPGRID").distinct().count() == parquetFileDF.count())
////
//
//val df = spark.read.csv("/home/prasoon/Downloads/")

}
