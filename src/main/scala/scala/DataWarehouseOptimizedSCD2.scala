package scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, lit, md5, row_number, when, concat}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import java.io.File
import java.util.Properties
import scala.ReadWriteToFromS3.{columns, conf}
object DataWarehouseOptimizedSCD2 extends  App{
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
  val columnsToBeMigratedwithHash = columnsToBeMigrated ++ Seq("hashstring")
  val columnsToBeMigratedwithHashFlag = columnsToBeMigrated ++ Seq("hashstring", "flag")
  val intermediaryColumns = columnsToBeMigrated.map(x => (x.concat("u")))
  val intermediaryColumnswithHashFlag = columnsToBeMigrated.map(x => (x.concat("u"))) ++ Seq("hashstringu", "flag")
  val intermediaryColumnswithHash = columnsToBeMigrated.map(x => (x.concat("u"))) ++ Seq("hashstringu")
  val intermediaryDeletewithHash = columnsToBeMigrated.map(x => (x.concat("d"))) ++ Seq("hashstringd")
  val intermediaryInsertwithHash = columnsToBeMigrated.map(x => (x.concat("i"))) ++ Seq("hashstringi")
  val intermediaryInsertwithHashFlag = columnsToBeMigrated.map(x => (x.concat("i"))) ++ Seq("hashstringi","flagi")
  val uniqueKey = "emp_id"
  val intermediaryupdateKey = "emp_idu"
  val intermediarydeleteKey = "emp_idd"
  val intermediaryinsertKey = "emp_idi"
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
  def readSelectedColumnsFromTable (dbschema: String, tablename: String, cols :Seq[String] =columnsToBeMigrated): DataFrame = {
    val df = spark.read.jdbc(jdbcurl, dbschema+"."+tablename,pgConnectionProperties).cache()
      .select(cols.map(m => col(m)): _*)
    df

  }
  def readTable (dbschema: String, tablename: String): DataFrame = {
    val df = spark.read.jdbc(jdbcurl, dbschema+"."+tablename,pgConnectionProperties).cache()
    df


  }
  def getHistoricalData(dataframe:DataFrame, cols :Seq[String] =columnsToBeMigrated ): DataFrame = {
    val df = dataframe
      .select(cols.map(m => col(m)): _*)
      .filter(col("Op")==='R' )
    df

  }
  def getInserts(dataframe:DataFrame, cols :Seq[String] =columnsToBeMigrated ): DataFrame = {
    val df = dataframe
      .select(cols.map(m => col(m)): _*)
      .filter(col("Op")==='I' or col("Op")==='i' )
    df

  }
  def getUpdates(dataframe:DataFrame, cols :Seq[String] =columnsToBeMigrated ): DataFrame = {
    val df = dataframe
      .select(cols.map(m => col(m)): _*)
      .filter(col("Op")==='U' or col("Op")==='u' )
    df

  }
  def getDeletes(dataframe:DataFrame, cols :Seq[String] =columnsToBeMigrated ): DataFrame = {
    val df = dataframe
      .select(cols.map(m => col(m)): _*)
      .filter(col("Op")==='D' or col("Op")==='d' )
    df

  }
  def addHashFlag(dataframe:DataFrame ): DataFrame = {
    val df = dataframe
      .withColumn("hashstring", md5(concat(col("ofc_location"))))
      .withColumn("flag",lit(true))
    df

  }
  def addHash(dataframe:DataFrame ): DataFrame = {
    val df = dataframe
      .withColumn("hashstring", md5(concat(col("ofc_location"))))
    df

  }
  def getRecentlyUpdatedData(dataframe:DataFrame): DataFrame ={
    val w = Window.partitionBy(col(uniqueKey)).orderBy(col("updated_at").desc)
    dataframe.withColumn("rn", row_number().over(w)).where(col("rn") === 1)
      .select(columnsToBeMigratedwithHash.map(m => col(m)): _*)
      .toDF(intermediaryColumnswithHash: _*)
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

  def addUpdatesAndDeletes(dataframe:DataFrame, tableAndInserts:DataFrame) ={
    val allupdates = addHashFlag(getUpdates(dataframe))
      .toDF(intermediaryColumnswithHashFlag: _*)
    val dftobeappendedunfiltered = allupdates.join(tableAndInserts,
          tableAndInserts(uniqueKey)===allupdates(intermediaryupdateKey) &&
          tableAndInserts("hashstring")===allupdates("hashstringu"),
          "leftouter")
    val dftobeappendedfiltered = dftobeappendedunfiltered
      .filter(col(uniqueKey).isNull)
      .select(intermediaryColumnswithHash.map(m => col(m)): _*)
      .withColumn("flag",lit(true))
      .toDF(columnsToBeMigratedwithHashFlag: _*)
    val incorrectflagdf = tableAndInserts.union(dftobeappendedfiltered)
    val recentlyUpdatedRows = getRecentlyUpdatedData(incorrectflagdf.filter(col("flag")===true))

    val df_after_update = incorrectflagdf
      .join(recentlyUpdatedRows,
              recentlyUpdatedRows(intermediaryupdateKey)===incorrectflagdf(uniqueKey), "leftouter")
      .withColumn("flag", when(col(intermediaryupdateKey).isNotNull &&
              col("hashstring")=!= col("hashstringu"), false).otherwise(col("flag")) )
      .select(columnsToBeMigratedwithHashFlag.map(m => col(m)): _*)
    val allDeletes = addHash(getDeletes(dataframe))
        .toDF(intermediaryDeletewithHash: _*)
    if (!allDeletes.take(1).isEmpty) {
      println("It has got delete")
      val df_after_del = df_after_update
        .join(allDeletes, col(uniqueKey)===col(intermediarydeleteKey)
            && col("hashstring")===col("hashstringd")
            && col("updated_at")<col("updated_atd"),
            "leftouter")
        .withColumn("flag", when(col(intermediarydeleteKey).isNotNull, false).otherwise(col("flag")))
        .select(columnsToBeMigratedwithHashFlag.map(m => col(m)): _*)
      println("final_Df after delete")
      df_after_del.show()
      writeData(df_after_del,loadTable)


    }
    else{
      println("It has no delete")
      df_after_update.show()
      writeData(df_after_update,loadTable)

    }
    val cnt = dataframe.count()
    val statement = "UPDATE "+auditTable+" SET lastfileread = lastfileadded, lastcountfolder=" +  cnt
    writesql(statement)


  }
  def previousTableWithNewInserts(dataframe:DataFrame) :DataFrame = {
    val dimensiontable = readTable("public","testcode")
    val allinserts = addHashFlag(getInserts(dataframe)).toDF(intermediaryInsertwithHashFlag : _*)
    val newinserts = allinserts
      .join(dimensiontable,
          allinserts(intermediaryinsertKey)===dimensiontable(uniqueKey) &&
          allinserts("hashstringi")===dimensiontable("hashstring"), "leftouter")
      .filter(col(uniqueKey).isNull).select(intermediaryInsertwithHashFlag.map(m => col(m)): _*)
      .toDF(columnsToBeMigratedwithHashFlag : _*)
    dimensiontable.union(newinserts)


  }
  if (lastfileRead == null ){
      val newfilesloaded = readcsvwithSchema(lastfileAdded,"true",customSchema)
      val dfinserts = addHashFlag(getInserts(newfilesloaded).union(getHistoricalData(newfilesloaded)))
      addUpdatesAndDeletes(newfilesloaded, dfinserts)
    }
    else{
      val newfilesloaded = readcsvwithSchema(lastfileRead,"true",customSchema)
      val cnt = newfilesloaded.count()
      val lastcount = lastCountFolder
      if (cnt != lastcount){
        val dfinserts = previousTableWithNewInserts(newfilesloaded)
        addUpdatesAndDeletes(newfilesloaded, dfinserts)

      }
      val statement = "UPDATE "+auditTable+" SET lastfileread = lastfileadded, lastcountfolder=" +  cnt
      writesql(statement)
  }
}
