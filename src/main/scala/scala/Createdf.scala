package scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}
object Createdf extends App{
  val conf = new SparkConf()
    .setAppName("TEstdfs")
    .set("log4j.logger.org.apache.kafka.clients.consumer.internals.SubscriptionState","WARN")
  val spark = SparkSession.builder()
    .master("local[*]")
    .config(conf)
    .getOrCreate()

  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", null), (null, "100000"), ("Scala", "3000"))
  val rdd = spark.sparkContext.parallelize(data)
  val df = rdd.toDF()
  df.select(coalesce(col("_1"),col("_2"))).show()


}
