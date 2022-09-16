package scala


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, when}
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.ReadWriteToFromS3.{columns, conf}

object DatawarehouseOptimized extends  App{
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

  val url = "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres"
  val pgTable = "public.AuditReadWrite"
  val pgConnectionProperties = new Properties()
  pgConnectionProperties.put("user","postgres")
  pgConnectionProperties.put("password","9473249664")
  //al pgCourseDataframe = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", pgTable,pgConnectionProperties)
  //  val query1 = s"SELECT * FROM AuditReadWrite"
  //  val query1df = spark.read.jdbc(url, query1, pgConnectionProperties)
  val var_name = "public.testcode"
  val query = s"SELECT * FROM $pgTable WHERE LOWER(tablename) LIKE LOWER('$var_name') "
  val df = spark
    .sqlContext
    .read
    .format("jdbc")
    .option("url", url)
    .option("user", "postgres")
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


  if (lastfileRead != lastfileAdded){
    if (lastfileRead == null ){
      println("I am inside lastfileRead == null")
      val df1 = spark.read.format("csv").option("header","true")
        .option("inferSchema","true")
        .load("s3a://twitter-khyber/test-code/"+lastfileAdded+"/*.csv")


      val pgConnectionProperties = new Properties()
      pgConnectionProperties.put("user","postgres")
      pgConnectionProperties.put("password","9473249664")

      val pgTable = "public.testcode"
      val jdbcUrl = "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres"


      /*
      filter op is r
      write in main table
      filter op is insert as dfinsert
      dfinsert directly append to table
      dftable is maintable as dataframe
      filter op[ as u as dfupdate and recent one
      chnage column names of dfupdate
      join dfupdate and dftable on prmary key
      change data wherever join is succesful
      take changed columns and overwrite


      */
      val df1historical = df1
        .select("emp_id","ofc_location","updated_at")
        .filter(col("Op")==='R' )
      df1historical.show()


      println("written inital load")

      val dfinserts = df1
        .select("emp_id","ofc_location","updated_at")
        .filter(col("Op")==='I' or col("Op")==='i')
        .union(df1historical)

      println("wrtitten newly inserts")
      dfinserts.show()

      val dfupdate = df1
        .filter(col("Op")==='U')

      //val dftable = dfinserts
      val w = Window.partitionBy(col("emp_id")).orderBy(col("updated_at").desc)
      val refined_df = dfupdate.withColumn("rn", row_number().over(w)).where(col("rn") === 1).select("emp_id","ofc_location","updated_at")//select all cols except rn
        .toDF("emp_idu","ofc_locationu","updated_atu")
      val temp_df = dfinserts.join(refined_df,refined_df("emp_idu")===dfinserts("emp_id"), "leftouter" )
      println("temp_df")
      temp_df.show()
      val temp_df1 =
        temp_df.withColumn("ofc_location",when(col("emp_idu").isNotNull, col("ofc_locationu")).otherwise(col("ofc_location")))
          .withColumn("updated_at",when(col("emp_idu").isNotNull, col("updated_atu")).otherwise(col("updated_at")))
      //        .withColumn("dftablecolumn3",when(col("refined_dfprimarykey").isNotNull, col("refindedfcol2")))
      //        .withColumn("dftablecolumn4",when(col("refined_dfprimarykey").isNotNull, col("refindedfcol4")))
      //        .withColumn("dftablecolumn5",when(col("refined_dfprimarykey").isNotNull, col("refindedfcol5")))
      //        .withColumn("dftablecolumn6",when(col("refined_dfprimarykey").isNotNull, col("refindedfcol6")))
      println("after join update")
      temp_df1.show()
      temp_df1.select("emp_id","ofc_location","updated_at").show()
      val final_df2 = temp_df1.select("emp_id","ofc_location","updated_at")
      final_df2.show()
      final_df2.printSchema()
      final_df2.write.format("jdbc")
        .option("url", "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres")
        .option("dbtable", "public.testcode")
        .option("user", "postgres")
        .option("password", "9473249664")
        .mode("overwrite")
        .save()
      /*
      df1
              .select("emp_id","ofc_location","updated_at")
              .filter(col("Op")==='R' )
              .write.format("jdbc")
              .mode("overwrite")
              .option("url", jdbcUrl)
              .option("dbtable", pgTable)
              .option("user", "postgres")
              .option("password", "9473249664")
              .save()
      * */

      println("done updates")
      final_df2.show()
      println("**************")



      //      val dfdelete = df1.filter(col("Op")==='D')
      //      val dftable2 = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", pgTable,pgConnectionProperties)
      //      val dfdelete_renamed = dfdelete.toDF("changedcolnames")
      //      dftable2.join(dfdelete_renamed,col("dftableprimarykey")===col("dfdelete_renamedprimarykey"), "leftouter")
      //        .filter(col("dfdelete_renamedprimarykey").isNotNull).select("dftable2cols")
      //        .write.format("jdbc")
      //        .mode("overwrite")
      //        .option("url", jdbcUrl)
      //        .option("dbtable", "TestCode")
      //        .option("user", "postgres")
      //        .option("password", "9473249664")
      //        .save()








      val cnt = df1.count()


      val url = "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres"
      val conn = DriverManager.getConnection(url,"postgres","9473249664")
      try {
        val stm = conn.createStatement()
        println("Updating auditread")

        val rs = stm.executeUpdate("UPDATE public.AuditReadWrite SET lastfileread = lastfileadded,lastcountfolder=" +  cnt)


      } finally {
        conn.close()
      }

    }
    else{

      val df1 = spark.read.format("CSV").option("header","true")
        .option("inferSchema","true")
        .load("s3a://twitter-khyber/test-code/" + lastfileRead + "/*.csv")
      println("I am inside lastfileRead is not null and lastfileRead != lastfileAdded")
      val cnt = df1.count()
      val lastcount = lastCountFolder
      val jdbcUrl = "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres"


      if (cnt != lastcount){


        val dfinsert = df1.filter(col("Op")==='I')
        val dftable = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", "public.testcode",pgConnectionProperties).cache()
        val dfinsert_renamed = dfinsert.select("emp_id","ofc_location","updated_at").toDF("emp_idi","ofc_locationi","updated_ati")
        println("df_insert is renamed")
        dfinsert_renamed.show()
        val dftobeinserted = dfinsert_renamed.join(dftable,dftable("emp_id")===dfinsert_renamed("emp_idi"), "leftouter")
          .filter(col("emp_id").isNull)
          .select("emp_idi","ofc_locationi","updated_ati").toDF("emp_id","ofc_location","updated_at")
        val dftableafterinsert = dftable.union(dftobeinserted)
        //dftable.unpersist()
        println("df_insert is written")

        val dfupdate = df1
          .filter(col("Op")==='U')

        //val dftable1 = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", "public.testcode",pgConnectionProperties).cache()
       // println("newdftable1 after insert")
        //dftable1.show()
        val w = Window.partitionBy(col("emp_id")).orderBy(col("updated_at").desc)
        val refined_df = dfupdate.withColumn("rn", row_number().over(w)).where(col("rn") === 1).select("emp_id","ofc_location","updated_at")//select all cols except rn
          .toDF("emp_idu","ofc_locationu","updated_atu")

        val temp_df = dftableafterinsert.join(refined_df,refined_df("emp_idu")===dftableafterinsert("emp_id"), "leftouter" )
        val temp_df1 =
          temp_df.withColumn("ofc_location",when(col("emp_idu").isNotNull, col("ofc_locationu")).otherwise(col("ofc_location")))
            .withColumn("updated_at",when(col("emp_idu").isNotNull, col("updated_atu")).otherwise(col("updated_at")))
        temp_df1.show()
        val dfupdated = temp_df1.select("emp_id","ofc_location","updated_at")
        dfupdated.show()


        val dfdelete = df1.filter(col("Op")==='D')
        if (!dfdelete.take(1).isEmpty) {
          println("NOpe I am delete  not empty")

          val dftable2 = dfupdated
          val dfdelete_renamed = dfdelete.select("emp_id","ofc_location","updated_at").toDF("emp_idd","ofc_locationd","updated_atd")
          val finaltable3 = dftable2.join(dfdelete_renamed,dftable2("emp_id") === dfdelete_renamed("emp_idd"), "leftouter")
            .filter(col("emp_idd").isNull)
            .select("emp_id","ofc_location","updated_at")
          finaltable3.show()
          finaltable3.write.format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", "public.testcode")
            .option("user", "postgres")
            .option("password", "9473249664")
            .mode("overwrite")
            .save()
          dftable2.unpersist()}

        else{
          println("No updates")
          dfupdated.write.format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", "public.testcode")
            .option("user", "postgres")
            .option("password", "9473249664")
            .mode("overwrite")
            .save()

        }
        val url = "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres"
        val conn = DriverManager.getConnection(url,"postgres","9473249664")
        try {
          val stm = conn.createStatement()

          val rs = stm.executeUpdate("UPDATE public.AuditReadWrite SET lastfileread = lastfileadded,lastcountfolder=" + 0 )


        } finally {
          conn.close()
        }

      }


    }


  }
  else if (lastfileRead == lastfileAdded){
    println("I am here when lastfileRead == lastfileAdded")
    val df1 = spark.read.format("csv").option("header","true")
      .option("inferSchema","true")
      .load("s3a://twitter-khyber/test-code/" + lastfileAdded + "/*.csv")
    df1.show()
    val lastcount = lastCountFolder
    val cnt = df1.count()
    val jdbcUrl = "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres"
    if (cnt!=lastcount) {








      val dfinsert = df1.filter(col("Op")==='I')
      val dftable = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", "public.testcode",pgConnectionProperties).cache()
      val dfinsert_renamed = dfinsert.select("emp_id","ofc_location","updated_at").toDF("emp_idi","ofc_locationi","updated_ati")
      println("df_insert is renamed")
      dfinsert_renamed.show()
      val tobeinserted = dfinsert_renamed.join(dftable,dftable("emp_id")===dfinsert_renamed("emp_idi"), "leftouter")
        .filter(col("emp_id").isNull)
        .select("emp_idi","ofc_locationi","updated_ati").toDF("emp_id","ofc_location","updated_at")
      val finaldf = tobeinserted.union(dftable)
      //dftable.unpersist()
      println("df_insert is written")


      val dfupdate = df1
        .filter(col("Op")==='U')

//      val dftable1 = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", "public.testcode",pgConnectionProperties).cache()
//      println("newdftable1 after insert")
//      dftable1.show()
      val w = Window.partitionBy(col("emp_id")).orderBy(col("updated_at").desc)
      val refined_df = dfupdate.withColumn("rn", row_number().over(w)).where(col("rn") === 1).select("emp_id","ofc_location","updated_at")//select all cols except rn
        .toDF("emp_idu","ofc_locationu","updated_atu")

      val temp_df = finaldf.join(refined_df,refined_df("emp_idu")===finaldf("emp_id"), "leftouter" )
      val temp_df1 =
        temp_df.withColumn("ofc_location",when(col("emp_idu").isNotNull, col("ofc_locationu")).otherwise(col("ofc_location")))
          .withColumn("updated_at",when(col("emp_idu").isNotNull, col("updated_atu")).otherwise(col("updated_at")))
      temp_df1.show()
      val final_updates_Df = temp_df1.select("emp_id","ofc_location","updated_at")
      println("Final_updates")
      final_updates_Df.show()
      //dftable1.unpersist()

      val dfdelete = df1.filter(col("Op")==='D')
      if (!dfdelete.take(1).isEmpty) {
        println("NOpe I am delete  not empty")
       // val dftable2 = spark.read.jdbc("jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres", "public.testcode", pgConnectionProperties).cache()
        val dfdelete_renamed = dfdelete.select("emp_id","ofc_location","updated_at").toDF("emp_idd","ofc_locationd","updated_atd")
        val final_table = final_updates_Df.join(dfdelete_renamed,final_updates_Df("emp_id") === dfdelete_renamed("emp_idd"), "leftouter")
          .filter(col("emp_idd").isNull)
          .select("emp_id","ofc_location","updated_at")

        final_table.write.format("jdbc")

          .option("url", jdbcUrl)
          .option("dbtable", "public.testCode")
          .option("user", "postgres")
          .option("password", "9473249664")
          .mode("overwrite")
          .save()




        println("No not empty")

      }
      else{
        final_updates_Df.show()


        final_updates_Df.write.format("jdbc")

          .option("url", "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres")
          .option("dbtable", "public.testCode")
          .option("user", "postgres")
          .option("password", "9473249664")
          .mode("overwrite")
          .save()
      }
      println("Presenting again")
      final_updates_Df.show()

      val url = "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres"
      val conn = DriverManager.getConnection(url,"postgres","9473249664")
      try {
        val stm = conn.createStatement()

        val rs = stm.executeUpdate("UPDATE public.AuditReadWrite SET lastcountfolder=" +  cnt )
        println("updated_Audir_REad_)Write")


      } finally {
        conn.close()
      }
    }








  }






}
