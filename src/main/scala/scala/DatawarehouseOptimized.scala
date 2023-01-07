package scala


import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, when}
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame

import java.io.File
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.ReadWriteToFromS3.{columns, conf}

object DatawarehouseOptimized extends App {
  val conf = new SparkConf()
    .setAppName("SparkKafkaStreaming")
    .set("spark.streaming.stopGracefullyShutdown", "true")
    .set("log4j.logger.org.apache.kafka.clients.consumer.internals.SubscriptionState", "WARN")

  val spark = SparkSession.builder()
    .master("local[*]")
    .config(conf)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", "AKIA5CMB477HF3Q5D63C")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "GhOV+gHGg7jwEg1jl0ioHhV3ijBlGvDmTFGdzRhB")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  val config = ConfigFactory.parseFile(new File("/home/prasoon/Downloads/scala-Practice/src/main/scala/scala/application.conf"))
  val targetdetails = config.getConfig("from-db-schema-table").getConfig("target")
  val jdbcurl = targetdetails.getString("jdbcurl")
  val auditTable = targetdetails.getString("auditTable")
  val loadTable = targetdetails.getString("loadTable")
  val dbuser = targetdetails.getString("dbUser")
  val dbpassword = targetdetails.getString("dbpassword")

  val pgConnectionProperties = new Properties()
  pgConnectionProperties.put("user", targetdetails.getString("dbUser"))
  pgConnectionProperties.put("password", targetdetails.getString("dbpassword"))


  val auditDetails = s"SELECT * FROM $auditTable WHERE LOWER(tablename) LIKE LOWER('$loadTable') "
  val auditDF = spark
    .sqlContext
    .read
    .format("jdbc")
    .option("url", jdbcurl)
    .option("user", dbuser)
    .option("password", dbpassword)
    .option("dbtable", s"($auditDetails) as t")
    .load().persist(StorageLevel.MEMORY_ONLY)

  val tabname = auditDF.take(1)(0).getAs[String]("tablename")
  val lastfileRead = auditDF.take(1)(0).getAs[Integer]("lastfileread")
  val lastfileAdded = auditDF.take(1)(0).getAs[Integer]("lastfileadded")
  val lastCountFolder = auditDF.take(1)(0).getAs[Integer]("lastcountfolder")

  val columnsToBeMigrated = Seq("emp_id", "ofc_location", "updated_at")
  val intermediaryColumns = columnsToBeMigrated.map(x => (x.concat("u")))
  val uniqueKey = "emp_id"


  def readcsv(filepath: Integer, isheader: String, isinferSchema: String): DataFrame = {
    val df = spark.read.format("csv").option("header", isheader)
      .option("inferSchema", isinferSchema)
      .load("/home/prasoon/its/datasets/" + filepath + "/*.csv")
    df

  }

  def getHistoricalData(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter(col("Op") === 'R')
      .select(columnsToBeMigrated.map(m => col(m)): _*)
  }

  def getInsertData(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter(col("Op") === 'I' or col("Op") === 'i')
      .select(columnsToBeMigrated.map(m => col(m)): _*)
  }

  def getUpdateData(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter(col("Op") === 'U')
  }

  def getDeleteData(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter(col("Op") === 'D')
  }

  def getLatestUpdatedData(dataFrame: DataFrame): DataFrame = {
    val w = Window.partitionBy(col(uniqueKey)).orderBy(col("updated_at").desc)
    dataFrame
      .withColumn("rn", row_number().over(w)).where(col("rn") === 1)
      .select(columnsToBeMigrated.map(m => col(m)): _*)
      .toDF(intermediaryColumns: _*)

  }

  def renameDF(dataframe: DataFrame): DataFrame = {
    dataframe
      .select(columnsToBeMigrated.map(m => col(m)): _*)
      .toDF(intermediaryColumns: _*)

  }


  def writeData(dataframe: DataFrame): Unit = {
    dataframe
      .select(columnsToBeMigrated.map(m => col(m)): _*)
      .write.format("jdbc")
      .option("url", jdbcurl)
      .option("dbtable", loadTable)
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

  def readFromTable(tablename: String): DataFrame = {
    spark.read.jdbc(jdbcurl, tablename, pgConnectionProperties).cache()

  }


  if (lastfileRead == null) {
    val lastfilereadDF = readcsv(lastfileAdded, "true", "true")
    val latestFileCount = lastfilereadDF.count()
    val historicalDF = getHistoricalData(lastfilereadDF)
    val tableafterinsertDF = getInsertData(lastfilereadDF).union(historicalDF)
    commonupdate(lastfilereadDF, tableafterinsertDF)
    updateAuditTable(lastfileRead, lastfileAdded, latestFileCount)

  }
  else {
    val lastfilereadDF = readcsv(lastfileRead, "true", "true")
    val latestFileCount = lastfilereadDF.count()
    if (latestFileCount != lastCountFolder) {
      val insertDF = getInsertData(lastfilereadDF)
      val dftable = readFromTable(loadTable)
      val dfinsert_renamed = renameDF(insertDF)
      val tobeinsertedDF = dfinsert_renamed.join(dftable, dftable(uniqueKey) === dfinsert_renamed("emp_idu"), "leftouter")
        .filter(col(uniqueKey).isNull)
        .select(intermediaryColumns.map(m => col(m)): _*)
        .toDF(columnsToBeMigrated: _*)
      val tableafterinsertDF = tobeinsertedDF.union(dftable)
      commonupdate(lastfilereadDF, tableafterinsertDF)



    }
    updateAuditTable(lastfileRead, lastfileAdded, latestFileCount)
  }

  def commonupdate(lastfilereadDF: DataFrame, tableafterinsertDF: DataFrame): Unit = {
    val updateDF = getUpdateData(lastfilereadDF)
    val latestUpdateDF = getLatestUpdatedData(updateDF)
    val intermediateInsertUpdateDF = tableafterinsertDF.join(latestUpdateDF, latestUpdateDF("emp_idu") === tableafterinsertDF("emp_id"), "leftouter")

    val updatedDF =
      intermediateInsertUpdateDF
        .withColumn("ofc_location", when(col("emp_idu").isNotNull, col("ofc_locationu")).otherwise(col("ofc_location")))
        .withColumn("updated_at", when(col("emp_idu").isNotNull, col("updated_atu")).otherwise(col("updated_at")))
        .select(columnsToBeMigrated.map(m => col(m)): _*)
    val deleteDF = getDeleteData(lastfilereadDF)
    if (!deleteDF.take(1).isEmpty) {
      val intermediateDeleteDF = renameDF(deleteDF)
      println("Updated df")
      updatedDF.show()
      val finalDF = updatedDF.join(intermediateDeleteDF, updatedDF(uniqueKey) === intermediateDeleteDF("emp_idu"), "leftouter")
        .filter(col("emp_idu").isNull)
        .select(columnsToBeMigrated.map(m => col(m)): _*)
      println("final df")
      finalDF.show()
      writeData(finalDF)
      updatedDF.unpersist()

    }

    else {
      updatedDF.show()
      writeData(updatedDF)
    }

  }

  def updateAuditTable(lastfileRead: Integer, lastfileAdded: Integer, latestFileCount: Long): Unit = {
    if (lastfileRead != lastfileAdded) {
      if (lastfileRead == null) {
        val statement = "UPDATE " + auditTable + " " +
          "SET lastfileread = lastfileadded," +
          "lastcountfolder=" + latestFileCount
        writesql(statement)
      }
      else {

        val statement = "UPDATE " + auditTable + " " +
          "SET lastfileread = lastfileadded," +
          "lastcountfolder=" + 0
        writesql(statement)


      }


    }
    else if (lastfileRead == lastfileAdded) {
      val statement = "UPDATE " + auditTable + " " +
        "SET lastfileread = lastfileadded," +
        "lastcountfolder=" + latestFileCount
      writesql(statement)
    }


  }
}



