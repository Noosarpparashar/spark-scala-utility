package scala

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{col, from_json, json_tuple}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.ReadWriteToFromS3.spark
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter



case class After (
                   tweet_id: String,
                   user_id: String,
                   tweet: String,
                   retweet_count: String,
                   temp_col: String
                 )

case class Fields (
`type`: String,
  fields: Option[Seq[Fields1]],
  optional: Boolean,
  name: Option[String],
  field: String
  )

  case class Fields1 (
  `type`: String,
  optional: Boolean,
  field: String
  )

  case class Payload (
                       before:  String,
                       after: After,
                       source: Source,
                       op: String,
                       ts_ms: Int,
                       transaction: String
                     )

  case class RootInterface (
                             schema: Schema,
                             payload: Payload
                           )

  case class Schema (
  `type`: String,
  fields: Seq[Fields],
  optional: Boolean,
  name: String
  )

  case class Source (
                      version: String,
                      connector: String,
                      name: String,
                      ts_ms: Int,
                      snapshot: String,
                      db: String,
                      sequence: String,
                      schema: String,
                      table: String,
                      txId: Int,
                      lsn: Int,
                      xmin: String
                    )

 //val schema = Encoders.product[LineItemData].schema
  object SparkStreaming extends  App {
  val conf = new SparkConf()
    .setAppName("SparkKafkaStreaming")
    .set("spark.streaming.stopGracefullyShutdown","true")
    .set("log4j.logger.org.apache.kafka.clients.consumer.internals.SubscriptionState","WARN")
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
  val df =spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe","khyber1.twitter.twittertweet")
    .option("startingOffsets","earliest")
    .load()
//  val stringdf = df.select(from_json($"value".cast("string"), schema).alias("value"))
//   .select(to_json($"value").alias("value")
//     // Write back
//     .writeStream
//     .format("kafka")
//     .option("kafka.bootstrap.servers", outservers)
//     .option("subscribe", outtopic)
//     .start()

//   val schema = Encoders.product[After].schema
//   val rawValues = df.selectExpr("CAST(value AS STRING)").as[String]
//   val jsonValues = rawValues.select(from_json($"value", schema) as "record")
//   val liData = jsonValues.select("record.*")
//   val query = liData.writeStream.queryName("temp").outputMode("append").format("console").start().awaitTermination()
  //df.printSchema()

//{"tweet_id":"13213","user_id":"454","tweet":"SDCSD","retweet_count":"DSFSD","temp_col":"6546700"}
    val format = "yyyyMMdd_HHmmss"

   val dtf = DateTimeFormatter.ofPattern(format)
   val stringdf = df.selectExpr("CAST(value as STRING)").as[String]
     .select(json_tuple(col("value"),"schema","payload") )
     .select(json_tuple(col("c1"),"after","op"))
     .select(col("c1"),json_tuple(col("c0"),"tweet_id","user_id","tweet","retweet_count","temp_col"))
     .toDF("op","tweet_id","user_id","tweet","retweet_count","temp_col")
   stringdf.writeStream
     .format("parquet")
//     .format("console")
   //  .outputMode("update")
     .option("checkpointLocation", "s3a://twitter-khyber/checkpoint/")
     .option("path", "s3a://twitter-khyber/test-result/"+ LocalDateTime.now().format(dtf))
     .start()
     .awaitTermination()


}
