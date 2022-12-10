package scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, lit, md5, row_number, when, concat}
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
object scd2Tablev2 extends App {

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
    //.hadoopConfiguration.set("fs.s3a.access.key", "AKIAYBA4HJFILSLHV3MZ")
  // Replace Key with your AWS secret key (You can find this on IAM
//  spark.sparkContext
//    .hadoopConfiguration.set("fs.s3a.secret.key", "CFHTnOdmuVF177rnJfIwBNeKZQhWdLl/pIp++M7z")
//  spark.sparkContext
//    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  val url = "jdbc:postgresql://localhost:5432/postgres"
  val pgTable = "public.AuditReadWrite"
  val pgConnectionProperties = new Properties()
  pgConnectionProperties.put("user","debezium")
  pgConnectionProperties.put("password","9473249664")
  //al pgCourseDataframe = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", pgTable,pgConnectionProperties)
  //  val query1 = s"SELECT * FROM AuditReadWrite"
  //  val query1df = spark.read.jdbc(url, query1, pgConnectionProperties)
  val var_name = "public.deltatable"
  val query = s"SELECT * FROM $pgTable WHERE LOWER(tablename) LIKE LOWER('$var_name') "
  val df = spark
    .sqlContext
    .read
    .format("jdbc")
    .option("url", url)
    .option("user", "debezium")
    .option("password", "9473249664")
    .option("dbtable", s"($query) as t")
    .load().persist(StorageLevel.MEMORY_ONLY)

  val tabname = df.take(1)(0).getAs[String]("tablename")
  val lastfileRead = df.take(1)(0).getAs[Integer]("lastfileread")
  val lastfileAdded = df.take(1)(0).getAs[Integer]("lastfileadded")
  val lastCountFolder = df.take(1)(0).getAs[Integer]("lastcountfolder")
  println(tabname)
  println(lastfileRead)
  println(lastfileAdded)
  println(lastCountFolder)
  val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
  val conn = DriverManager.getConnection(jdbcUrl,"debezium","9473249664")
  if (lastfileRead != lastfileAdded){
    if (lastfileRead == null ){
      println("I am inside lastfileRead == null")
      val df1 = spark.read.format("csv").option("header","true")
        .option("inferSchema","true")
        .load("/home/prasoon/its/datasets/"+lastfileAdded+"/*.csv")

df1.show()
      val pgConnectionProperties = new Properties()
      pgConnectionProperties.put("user","debezium")
      pgConnectionProperties.put("password","9473249664")



      df1.write.format("jdbc")

        .option("url", jdbcUrl)
        .option("dbtable", var_name)
        .option("user", "debezium")
        .option("password", "9473249664")
        .mode("overwrite")
        .save()

      println("Delta load completed")



      //val url = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"
      //val conn = DriverManager.getConnection(jdbcUrl,"debezium","9473249664")


      try {
        val stm = conn.createStatement()
        println("INserting data to table")

        val rs = stm.executeUpdate("INSERT INTO public.mainTable (emp_id,ofc_location,updated_at, flagg )" +
          "\n(SELECT dt.emp_id, dt.ofc_location,dt.updated_at,True from public.deltatable dt where (op = 'I' or op = 'R') " +
          "and \n md5(ROW(DT.EMP_ID,DT.OFC_LOCATION,DT.UPDATED_AT)::TEXT) NOT IN (SELECT md5(ROW(EMP_ID,OFC_LOCATION,UPDATED_AT)::TEXT) from public.mainTable ) )")


      }

      println("wrtitten newly updates")

      //
      try {
        val stm = conn.createStatement()
        println("Upserts data to table")

        stm.executeUpdate("INSERT INTO public.mainTable (emp_id,ofc_location,updated_at, flagg )\n" +
          "(SELECT DT.EMP_ID,DT.OFC_LOCATION, DT.UPDATED_AT,False FROM DELTATABLE DT  INNER JOIN " +
          "(select emp_id, max(updateD_At) AS MAX_UPDATED from PUBLIC.MAINTABLE GROUP BY EMP_ID ) T1" +
          "\nON T1.EMP_ID = DT.EMP_ID AND \nDT.UPDATED_AT>T1.MAX_UPDATED\nWHERE DT.OP = 'U')")

        stm.executeUpdate("UPDATE public.mainTable  MT\nSET FLAGG = CASE \n\t\tWHEN MT.UPDATED_AT<>T1.MAX_UPDATED THEN False\n\t\tELSE True\n\t\tend\n\t\tFROM" +
          "  (select emp_id, max(updateD_At) AS MAX_UPDATED from PUBLIC.MAINTABLE GROUP BY EMP_ID ) T1\n\t\tWHERE  T1.EMP_ID = MT.EMP_ID ")


      }
      println("wrtitten newly updates")


      try {
        val stm = conn.createStatement()
        println("delete data from table")



        stm.executeUpdate("UPDATE public.mainTable  MT\nSET FLAGG = False\n\t\t\n\t\tFROM " +
          " (SELECT DT.EMP_ID,DT.OFC_LOCATION, DT.UPDATED_AT,False FROM DELTATABLE DT WHERE DT.OP = 'D' ) T1\n\t\tWHERE  T1.EMP_ID = MT.EMP_ID  AND T1.UPDATED_AT > MT.UPDATED_aT" +
          " \n\t\tAND md5(ROW(T1.OFC_LOCATION)::TEXT) = md5(ROW(MT.OFC_LOCATION)::TEXT)")


      }

      val cnt = df1.count()
      //val url = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"
      //val conn = DriverManager.getConnection(url,"postgres","9473249664")

      try {
        val stm = conn.createStatement()
        println("Updating auditread")

        stm.executeUpdate("UPDATE public.AuditReadWrite SET lastfileread = lastfileadded,lastcountfolder=" +  cnt)
        stm.executeUpdate("TRUNCATE TABLE public.deltatable")


      } finally {
        conn.close()
      }



    }
    else{
      val df1 = spark.read.format("csv").option("header","true")
        .option("inferSchema","true")
        .load("/home/prasoon/its/datasets/"+lastfileRead+"/*.csv")
      println("I am inside lastfileRead is not null and lastfileRead != lastfileAdded")
      val cnt = df1.count()
      val lastcount = lastCountFolder


      println(cnt)
      if (cnt != lastcount) {
        val pgConnectionProperties = new Properties()
        pgConnectionProperties.put("user", "debezium")
        pgConnectionProperties.put("password", "9473249664")

        val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
        df1.write.format("jdbc")

          .option("url", jdbcUrl)
          .option("dbtable", var_name)
          .option("user", "debezium")
          .option("password", "9473249664")
          .mode("overwrite")
          .save()




        //
        //        val conn = DriverManager.getConnection(jdbcUrl,"debezium","9473249664")


        try {
          val stm = conn.createStatement()
          println("INserting data to table")

          stm.executeUpdate("INSERT INTO public.mainTable (emp_id,ofc_location,updated_at, flagg )\n(SELECT dt.emp_id, dt.ofc_location,dt.updated_at,True from public.deltatable dt where (op = 'I' or op = 'R') and \n " +
            "md5(ROW(DT.EMP_ID,DT.OFC_LOCATION,DT.UPDATED_AT)::TEXT) NOT IN (SELECT md5(ROW(EMP_ID,OFC_LOCATION,UPDATED_AT)::TEXT) from public.mainTable ) )")


        }
        //


        try {
          val stm = conn.createStatement()
          println("Upserts data to table")

          stm.executeUpdate("INSERT INTO public.mainTable (emp_id,ofc_location,updated_at, flagg )\n" +
            "(SELECT DT.EMP_ID,DT.OFC_LOCATION, DT.UPDATED_AT,False FROM DELTATABLE DT  INNER JOIN " +
            "(select emp_id, max(updateD_At) AS MAX_UPDATED from PUBLIC.MAINTABLE GROUP BY EMP_ID ) T1" +
            "\nON T1.EMP_ID = DT.EMP_ID AND \nDT.UPDATED_AT>T1.MAX_UPDATED\nWHERE DT.OP = 'U')")

          stm.executeUpdate("UPDATE public.mainTable  MT\nSET FLAGG = CASE \n\t\tWHEN MT.UPDATED_AT<>T1.MAX_UPDATED THEN False\n\t\tELSE True\n\t\tend\n\t\tFROM" +
            "  (select emp_id, max(updateD_At) AS MAX_UPDATED from PUBLIC.MAINTABLE GROUP BY EMP_ID ) T1\n\t\tWHERE  T1.EMP_ID = MT.EMP_ID ")


        }

        println("wrtitten newly updates")


        try {
          val stm = conn.createStatement()
          println("delete data from table")


          stm.executeUpdate("UPDATE public.mainTable  MT\nSET FLAGG = False\n\t\t\n\t\tFROM " +
            " (SELECT DT.EMP_ID,DT.OFC_LOCATION, DT.UPDATED_AT,False FROM DELTATABLE DT WHERE DT.OP = 'D' ) T1\n\t\tWHERE  T1.EMP_ID = MT.EMP_ID  AND T1.UPDATED_AT > MT.UPDATED_aT" +
            " \n\t\tAND md5(ROW(T1.OFC_LOCATION)::TEXT) = md5(ROW(MT.OFC_LOCATION)::TEXT)")


        }

        //        val cnt = df1.count()
        //        val url = "jdbc:postgresql://localhost:5432/postgres"
        // val conn = DriverManager.getConnection(url,"postgres","9473249664")
      }
        try {
          val stm = conn.createStatement()
          println("Updating auditread")

          stm.executeUpdate("UPDATE public.AuditReadWrite SET lastfileread = lastfileadded,lastcountfolder=" +  cnt)
          stm.executeUpdate("TRUNCATE TABLE public.deltatable")


        } finally {
          conn.close()
        }









    }
  }

  else{

    val df1 = spark.read.format("csv").option("header","true")
      .option("inferSchema","true")
      .load("/home/prasoon/its/datasets/"+lastfileRead+"/*.csv")
    val cnt = df1.count()


    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","debezium")
    pgConnectionProperties.put("password","9473249664")
//    val pgConnectionProperties = new Properties()
//    pgConnectionProperties.put("user","debezium")
//    pgConnectionProperties.put("password","9473249664")

    val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    df1.write.format("jdbc")

      .option("url", jdbcUrl)
      .option("dbtable", var_name)
      .option("user", "debezium")
      .option("password", "9473249664")
      .mode("overwrite")
      .save()

//    val pgTable = "public.testcode"
//    val jdbcUrl = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"



//    val dftable  = spark.read.jdbc("jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres", "public.testcode",pgConnectionProperties).cache()
//      .select("emp_id","ofc_location","updated_at","hashstring","flag")
//




    try {
      val stm = conn.createStatement()
      println("INserting data to table")

      stm.executeUpdate("INSERT INTO public.mainTable (emp_id,ofc_location,updated_at, flagg )\n(SELECT dt.emp_id, dt.ofc_location,dt.updated_at,True from public.deltatable dt where (op = 'I' or op = 'R') and \n " +
        "md5(ROW(DT.EMP_ID,DT.OFC_LOCATION,DT.UPDATED_AT)::TEXT) NOT IN (SELECT md5(ROW(EMP_ID,OFC_LOCATION,UPDATED_AT)::TEXT) from public.mainTable ) )")


    }


    try {
      val stm = conn.createStatement()
      println("Upserts data to table")

      stm.executeUpdate("INSERT INTO public.mainTable (emp_id,ofc_location,updated_at, flagg )\n" +
        "(SELECT DT.EMP_ID,DT.OFC_LOCATION, DT.UPDATED_AT,False FROM DELTATABLE DT  INNER JOIN " +
        "(select emp_id, max(updateD_At) AS MAX_UPDATED from PUBLIC.MAINTABLE GROUP BY EMP_ID ) T1" +
        "\nON T1.EMP_ID = DT.EMP_ID AND \nDT.UPDATED_AT>T1.MAX_UPDATED\nWHERE DT.OP = 'U')")

      stm.executeUpdate("UPDATE public.mainTable  MT\nSET FLAGG = CASE \n\t\tWHEN MT.UPDATED_AT<>T1.MAX_UPDATED THEN False\n\t\tELSE True\n\t\tend\n\t\tFROM" +
        "  (select emp_id, max(updateD_At) AS MAX_UPDATED from PUBLIC.MAINTABLE GROUP BY EMP_ID ) T1\n\t\tWHERE  T1.EMP_ID = MT.EMP_ID ")


    }

    println("wrtitten newly updates")


    try {
      val stm = conn.createStatement()
      println("delete data from table")



      stm.executeUpdate("UPDATE public.mainTable  MT\nSET FLAGG = False\n\t\t\n\t\tFROM " +
        " (SELECT DT.EMP_ID,DT.OFC_LOCATION, DT.UPDATED_AT,False FROM DELTATABLE DT WHERE DT.OP = 'D' ) T1\n\t\tWHERE  T1.EMP_ID = MT.EMP_ID  AND T1.UPDATED_AT > MT.UPDATED_aT" +
        " \n\t\tAND md5(ROW(T1.OFC_LOCATION)::TEXT) = md5(ROW(MT.OFC_LOCATION)::TEXT)")


    }




    try {
      val stm = conn.createStatement()
      println("Updating auditread")

      stm.executeUpdate("UPDATE public.AuditReadWrite SET lastfileread = lastfileadded,lastcountfolder=" +  cnt)
      stm.executeUpdate("TRUNCATE TABLE public.deltatable")


    } finally {
      conn.close()
    }


  }
}
