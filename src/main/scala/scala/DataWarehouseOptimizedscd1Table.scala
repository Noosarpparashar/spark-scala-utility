package scala


import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, row_number, when}
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.ConfigFactory
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import java.io.File

object DataWarehouseOptimizedscd1Table extends App{
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
    .hadoopConfiguration.set("fs.s3a.access.key", "AKIA5CMB477HF3Q5D63C")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "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")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  val config = ConfigFactory.parseFile(new File("/home/prasoon/Downloads/scala-Practice/src/main/scala/scala/application.conf"))
  val targetdetails = config.getConfig("from-db-schema-table-scd1-table").getConfig("target")
  val jdbcurl = targetdetails.getString("jdbcurl")
  val auditTable = targetdetails.getString("auditTable")
  val loadTable = targetdetails.getString("loadTable")
  val dbuser = targetdetails.getString("dbUser")
  val dbpassword = targetdetails.getString("dbpassword")
  val deltatable = targetdetails.getString("deltatable")

  val pgConnectionProperties = new Properties()
  pgConnectionProperties.put("user", targetdetails.getString("dbUser"))
  pgConnectionProperties.put("password", targetdetails.getString("dbpassword"))

  val auditDetails = s"SELECT * FROM $auditTable WHERE LOWER(tablename) LIKE LOWER('$loadTable') "

  val df = spark
    .sqlContext
    .read
    .format("jdbc")
    .option("url", jdbcurl)
    .option("user", dbuser)
    .option("password", dbpassword)
    .option("dbtable", s"($auditDetails) as t")
    .load().persist(StorageLevel.MEMORY_ONLY)
  df.show()
  println(df.printSchema())
  val tabname = df.take(1)(0).getAs[String]("tablename")
  val lastfileRead = df.take(1)(0).getAs[Integer]("lastfileread")
  val lastfileAdded = df.take(1)(0).getAs[Integer]("lastfileadded")
  val lastCountFolder = df.take(1)(0).getAs[Integer]("lastcountfolder")
  val columnsToBeMigrated = Seq("emp_id", "ofc_location", "updated_at")
  val intermediaryColumns = columnsToBeMigrated.map(x => (x.concat("u")))
  val uniqueKey = "emp_id"
  println(tabname)
  println(lastfileRead)
  println(lastfileAdded)
  println(lastCountFolder)

  val customSchema = StructType(Array(
    StructField("OP", StringType, true),
    StructField("EMP_ID", StringType, true),
    StructField("OFC_LOCATION", StringType, true),
    StructField("UPDATED_AT", IntegerType, true))
  )

  def readcsv(filepath: Integer, isheader: String, isinferSchema: String): DataFrame = {
    val df = spark.read.format("csv").option("header", isheader)
      .option("inferSchema", isinferSchema)
      .load("/home/prasoon/its/datasets/" + filepath + "/*.csv")
    df

  }
  def readcsvwithSchema(filepath: Integer, isheader: String, schema: StructType): DataFrame = {
    val df = spark.read.format("csv").option("header", isheader)
      .schema(customSchema)
      .load("/home/prasoon/its/datasets/" + filepath + "/*.csv")
    df

  }

  def writeData(dataframe: DataFrame, table:String): Unit = {
    dataframe
     // .select(columnsToBeMigrated.map(m => col(m)): _*)
      .write.format("jdbc")
      .option("truncate", "true")
      .option("url", jdbcurl)
      .option("dbtable", table)
      .option("user", dbuser)
      .option("password", dbpassword)
      .mode("overwrite")
      .save()

  }
  def writesql(statement: String): Unit = {
    val conn = DriverManager.getConnection(jdbcurl, dbuser, dbpassword)
    try {
      val stm = conn.createStatement()
      stm.executeUpdate(statement)
    }
    finally {
      conn.close()
    }

  }




  def filetoread(lastfileRead: Integer, lastfileAdded: Integer):Integer ={
    if (lastfileRead !=lastfileAdded && lastfileRead != null){
      val toberead = lastfileRead
      println(toberead)
      toberead
      }
    else if (lastfileRead == lastfileAdded || lastfileRead == null) {
      val toberead = lastfileAdded
      println(toberead)
      toberead

    }
    else {
      2
    }


  }
  println(lastfileRead)
  println(lastfileAdded)
    val df1 = readcsvwithSchema(filetoread(lastfileRead,lastfileAdded), "true", customSchema)
  df1.show()
    val lastcount = lastCountFolder
    val cnt = df1.count()
    if (cnt!=lastcount) {
      writeData(df1,deltatable)
      val queryinsert = "INSERT INTO "+ loadTable+" (emp_id,ofc_location,updated_at ) " +
        "(SELECT dt.emp_id, dt.ofc_location,dt.updated_at from "+deltatable+" dt where op = 'I' or op = 'R' ) " +
        "ON CONFLICT (emp_id) DO NOTHING"
      writesql(queryinsert)
      println("done historical load and inserts")
      val queryupdate = "INSERT INTO "+loadTable+" (emp_id,ofc_location,updated_at )(SELECT sq.emp_id,sq.ofc_location,sq.updated_at  FROM " +
        "(SELECT *, ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY updated_at desc) row_num from "+deltatable+" where op = 'U' ) sq " +
        "WHERE row_num = 1) ON CONFLICT (emp_id) DO UPDATE SET ofc_location =  excluded.ofc_location WHERE "+loadTable+".updated_at < excluded.updated_at"

      writesql(queryupdate)
      val querydelete = "DELETE   FROM  "+loadTable+" mt USING "+deltatable+" dt WHERE  mt.emp_id = dt.emp_id and  dt.op = 'D' AND dt.updated_at > mt.updated_at"
      writesql(querydelete)

    }
    val statement = "UPDATE "+auditTable+" SET lastfileread = lastfileadded, lastcountfolder=" +  0
    writesql(statement)






}
