package scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{col, from_json, json_tuple}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object ReadWriteToFromS3 extends App{
  val conf = new SparkConf()
    .setAppName("SparkKafkaStreaming")
    .set("spark.streaming.stopGracefullyShutdown","true")
    .set("log4j.logger.org.apache.kafka.clients.consumer.internals.SubscriptionState","WARN")
//    .set("fs.s3.access.key", "AKIA5CMB477HEL66S2UD")
//    .set("fs.s3.secret.key", "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")
//    .set("fs.s3.endpoint", "https://twitter-khyber.s3.amazonaws.com")
   // .set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  val spark = SparkSession.builder()
    .master("local[*]")
    .config(conf)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", "AKIA5CMB477HEL66S2UD")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "MsNt0BTvhl5GCD+JeGfmbPbmNq5kvqUXaamE8fGG")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
  val rdd = spark.sparkContext.parallelize(data)
  val dfFromRDD1 = rdd.toDF("language","users_count")
  //dfFromRDD1.show()
  val df1 = spark.read.format("csv")
      .option("inferSchema","true")
      .load("s3a://twitter-khyber/test-result/*/*.csv")
  println("hello")
  println(df1.count())
    df1.show()
//  val df1 = spark.read.format("parquet").option("header","true")
//  //  .option("inferSchema","true").
//    .load("s3a://twitter-khyber/test-result/20220822_155324/*.parquet")
//  df1.show()
//  dfFromRDD1
//    .repartition(1)
//    .write
//    .format("csv")
//    .option("header","true")
//    .save("s3a://twitter-khyber/test-result/2023.csv")
//
//
//
}
