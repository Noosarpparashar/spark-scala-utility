package scala


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.ReadWriteToFromS3.{columns, conf}

object scd1Tablev2 extends App{
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
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "GhOV+gHGg7jwEg1jl0ioHhV3ijBlGvDmTFGdzRhB")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  val url = "jdbc:postgresql://localhost:5432/postgres"
  val pgTable = "public.\"AUDIT_READ_WRITE\""
  val pgConnectionProperties = new Properties()
  pgConnectionProperties.put("user","debezium")
  pgConnectionProperties.put("password","9473249664")
  //al pgCourseDataframe = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", pgTable,pgConnectionProperties)
  //  val query1 = s"SELECT * FROM AuditReadWrite"
  //  val query1df = spark.read.jdbc(url, query1, pgConnectionProperties)
  val var_name = "public.deltatable"
  //val query = s"select * from public.\"AUDIT_READ_WRITE\""
  val query = s"SELECT * FROM $pgTable WHERE LOWER(tablename) LIKE LOWER('$var_name') "
  //WHERE LOWER(tablename) LIKE LOWER('$var_name')
  val df = spark
    .sqlContext
    .read
    .format("jdbc")
    .option("url", url)
    .option("user", "debezium")
    .option("password", "9473249664")
    .option("inferschema", "true")
    .option("dbtable", s"($query) as t")
    .load().persist(StorageLevel.MEMORY_ONLY)
  df.show()
  println(df.printSchema())
  val tabname = df.take(1)(0).getAs[String]("tablename")
  val lastfileRead = df.take(1)(0).getAs[Integer]("lastfileread")
  val lastfileAdded = df.take(1)(0).getAs[Integer]("lastfileadded")
  val lastCountFolder = df.take(1)(0).getAs[Integer]("lastcountfolder")
  println(tabname)
  println(lastfileRead)
  println(lastfileAdded)
  println(lastCountFolder)


  if (lastfileRead != lastfileAdded){
    //val url = "jdbc:localhost:5432/postgres"
    val conn = DriverManager.getConnection(url,"debezium","9473249664")

    if (lastfileRead == null ){
      println("I am inside lastfileRead == null")
      val df1 = spark.read.format("csv").option("header","true")
        .option("inferSchema","true")
        .load("/home/prasoon/Downloads/testdata/"+lastfileAdded+"/*.csv")


      val pgConnectionProperties = new Properties()
      pgConnectionProperties.put("user","postgres")
      pgConnectionProperties.put("password","9473249664")

      val pgTable = "public.testcode"
      val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"



        df1.write.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "public.\"deltaTable\"")
        .option("user", "debezium")
        .option("password", "9473249664")
        .mode("overwrite")
        .save()


      println("Filled Delta table")


      val queryinsert = s"INSERT INTO public.\"mainTable\" (empid,ofc_location,updated_at ) " +
        s"(SELECT dt.empid, dt.ofc_location,dt.updated_at from public.\"deltaTable\" dt where op = 'I' or op = 'R' ) " +
        s"ON CONFLICT (empid) DO NOTHING"

      try {
        val stm = conn.createStatement()
        println("INserting from delta to main")

        val rs = stm.executeUpdate(queryinsert)


      } finally {
        //conn.close()
      }


      println("done historical load and inserts")
      val queryupdate = s"INSERT INTO public.\"mainTable\" (empid,ofc_location,updated_at )(SELECT sq.empid,sq.ofc_location,sq.updated_at  FROM " +
        s"(SELECT *, ROW_NUMBER() OVER (PARTITION BY empid ORDER BY updated_at desc) row_num from public.\"deltaTable\" where op = 'U' ) sq " +
        s"WHERE row_num = 1) ON CONFLICT (empid) DO UPDATE SET ofc_location =  excluded.ofc_location WHERE public.\"mainTable\".updated_at < excluded.updated_at"

      try {
        val stm = conn.createStatement()
        println("Updating from delta to main")

        val rs = stm.executeUpdate(queryupdate)


      } finally {
        //conn.close()
      }
      println("done update query")

      val querydelete = s"DELETE   FROM  public.\"mainTable\" mt USING public.\"deltaTable\" dt WHERE  mt.empid = dt.empid and  dt.op = 'D' AND dt.updated_at > mt.updated_at"
      try {
        val stm = conn.createStatement()
        println("delete main")

        val rs = stm.executeUpdate(querydelete)


      } finally {
       // conn.close()
      }




      val cnt = df1.count()



      try {
        val stm = conn.createStatement()
        println("Updating auditread")

        val rs = stm.executeUpdate("UPDATE public.\"AUDIT_READ_WRITE\"SET lastfileread = lastfileadded, lastcountfolder=" +  cnt)


      } finally {
        conn.close()
      }

    }
    else{

      val simpleSchema = StructType(Array(
        StructField("op",StringType,true),
        StructField("empid",IntegerType,true),
        StructField("ofc_location",StringType,true),
        StructField("updated_at", IntegerType, true)
      ))
      println("I am here when lastfileRead != lastfileAdded")
      val df1 = spark.read.format("csv").option("header","true")
        .schema(simpleSchema)
        .load("/home/prasoon/Downloads/testdata/" + lastfileRead + "/*.csv")
      df1.show()
      val lastcount = lastCountFolder
      val cnt = df1.count()
      val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
      if (cnt!=lastcount) {
        println("Coun is not equalto lastcount")
        val pgConnectionProperties = new Properties()
        pgConnectionProperties.put("user","postgres")
        pgConnectionProperties.put("password","9473249664")

        val pgTable = "public.testcode"
        val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
        println(df1.printSchema())
        df1.show()


        df1.write.format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", "public.\"deltaTable\"")
          .option("user", "debezium")
          .option("password", "9473249664")
          .mode("overwrite")
          .save()

        val conn = DriverManager.getConnection(url,"debezium","9473249664")
        println("Filled Delta table")


        val queryinsert = s"INSERT INTO public.\"mainTable\" (empid,ofc_location,updated_at ) " +
          s"(SELECT dt.empid, dt.ofc_location,dt.updated_at from public.\"deltaTable\" dt where op = 'I' or op = 'R' ) " +
          s"ON CONFLICT (empid) DO NOTHING"

        try {
          val stm = conn.createStatement()
          println("INserting from delta to main")

          val rs = stm.executeUpdate(queryinsert)


        } finally {
          //conn.close()
        }


        println("done historical load and inserts")
        val queryupdate = s"INSERT INTO public.\"mainTable\" (empid,ofc_location,updated_at )(SELECT sq.empid,sq.ofc_location,sq.updated_at  FROM " +
          s"(SELECT *, ROW_NUMBER() OVER (PARTITION BY empid ORDER BY updated_at desc) row_num from public.\"deltaTable\" where op = 'U' ) sq " +
          s"WHERE row_num = 1) ON CONFLICT (empid) DO UPDATE SET ofc_location =  excluded.ofc_location WHERE public.\"mainTable\".updated_at < excluded.updated_at"

        try {
          val stm = conn.createStatement()
          println("Updating from delta to main")

          val rs = stm.executeUpdate(queryupdate)


        } finally {
          //conn.close()
        }
        println("done update query")

        val querydelete = s"DELETE   FROM  public.\"mainTable\" mt USING public.\"deltaTable\" dt WHERE  mt.empid = dt.empid and  dt.op = 'D' AND dt.updated_at > mt.updated_at"
        try {
          val stm = conn.createStatement()
          println("delete main")

          val rs = stm.executeUpdate(querydelete)


        } finally {
          // conn.close()
        }




        val cnt = df1.count()



        try {
          val stm = conn.createStatement()
          println("Updating auditread")

          val rs = stm.executeUpdate("UPDATE public.\"AUDIT_READ_WRITE\"SET lastfileread = lastfileadded, lastcountfolder=" +  0)

        } finally {
          conn.close()
        }









      }




    }


  }
  else if (lastfileRead == lastfileAdded){
    val simpleSchema = StructType(Array(
      StructField("op",StringType,true),
      StructField("empid",IntegerType,true),
      StructField("ofc_location",StringType,true),
      StructField("updated_at", IntegerType, true)
    ))
    println("I am here when lastfileRead == lastfileAdded")
    val df1 = spark.read.format("csv").option("header","true")
      .schema(simpleSchema)
      .load("/home/prasoon/Downloads/testdata/" + lastfileAdded + "/*.csv")
    df1.show()
    val lastcount = lastCountFolder
    val cnt = df1.count()
    val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    if (cnt!=lastcount) {
      val pgConnectionProperties = new Properties()
      pgConnectionProperties.put("user","postgres")
      pgConnectionProperties.put("password","9473249664")

      val pgTable = "public.testcode"
      val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
      println(df1.printSchema())
      df1.show()


      df1.write.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "public.\"deltaTable\"")
        .option("user", "debezium")
        .option("password", "9473249664")
        .mode("overwrite")
        .save()

      val conn = DriverManager.getConnection(url,"debezium","9473249664")
      println("Filled Delta table")


      val queryinsert = s"INSERT INTO public.\"mainTable\" (empid,ofc_location,updated_at ) " +
        s"(SELECT dt.empid, dt.ofc_location,dt.updated_at from public.\"deltaTable\" dt where op = 'I' or op = 'R' ) " +
        s"ON CONFLICT (empid) DO NOTHING"

      try {
        val stm = conn.createStatement()
        println("INserting from delta to main")

        val rs = stm.executeUpdate(queryinsert)


      } finally {
        //conn.close()
      }


      println("done historical load and inserts")
      val queryupdate = s"INSERT INTO public.\"mainTable\" (empid,ofc_location,updated_at )(SELECT sq.empid,sq.ofc_location,sq.updated_at  FROM " +
        s"(SELECT *, ROW_NUMBER() OVER (PARTITION BY empid ORDER BY updated_at desc) row_num from public.\"deltaTable\" where op = 'U' ) sq " +
        s"WHERE row_num = 1) ON CONFLICT (empid) DO UPDATE SET ofc_location =  excluded.ofc_location WHERE public.\"mainTable\".updated_at < excluded.updated_at"

      try {
        val stm = conn.createStatement()
        println("Updating from delta to main")

        val rs = stm.executeUpdate(queryupdate)


      } finally {
        //conn.close()
      }
      println("done update query")

      val querydelete = s"DELETE   FROM  public.\"mainTable\" mt USING public.\"deltaTable\" dt WHERE  mt.empid = dt.empid and  dt.op = 'D' AND dt.updated_at > mt.updated_at"
      try {
        val stm = conn.createStatement()
        println("delete main")

        val rs = stm.executeUpdate(querydelete)


      } finally {
        // conn.close()
      }




      val cnt = df1.count()



      try {
        val stm = conn.createStatement()
        println("Updating auditread")

        val rs = stm.executeUpdate("UPDATE public.\"AUDIT_READ_WRITE\"SET lastfileread = lastfileadded, lastcountfolder=" +  cnt)


      } finally {
        conn.close()
      }









    }








  }

}
