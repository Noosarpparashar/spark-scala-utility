package scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, lit, md5, row_number, when, concat}
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
object DatawarehouseSCD2 extends App {

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
    .hadoopConfiguration.set("fs.s3a.access.key", "AKIAYBA4HJFILSLHV3MZ")
  // Replace Key with your AWS secret key (You can find this on IAM
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "CFHTnOdmuVF177rnJfIwBNeKZQhWdLl/pIp++M7z")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  val url = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"
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
        .load("s3a://twitter-khyber1/test-code/"+lastfileAdded+"/*.csv")


      val pgConnectionProperties = new Properties()
      pgConnectionProperties.put("user","postgres")
      pgConnectionProperties.put("password","9473249664")

      val pgTable = "public.testcode"
      val jdbcUrl = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"



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
      val dfinserts1 = dfinserts.withColumn("hashstring", md5(concat(col("ofc_location"), col("updated_at"))))
      .withColumn("flag",lit(true))
      dfinserts1.show()
//
val dfupdate = df1
  .filter(col("Op")==='U')
  .select("emp_id","ofc_location","updated_at")
  .withColumn("hashstring", md5(concat(col("ofc_location"), col("updated_at"))))
  .withColumn("flag",lit(true))
  .toDF("emp_idu","ofc_locationu","updated_atu","hashstringu","flag")
//

      val dftobeappendedunfiltered = dfupdate.join(dfinserts1,dfinserts1("emp_id")===dfupdate("emp_idu") && dfinserts1("hashstring")===dfupdate("hashstringu"),"leftouter")
      val dftobeappendedfiltered = dftobeappendedunfiltered.filter(col("emp_id").isNull).select("emp_idu","ofc_locationu","updated_atu","hashstringu")
        .withColumn("flag",lit(true)).toDF("emp_id","ofc_location","updated_at","hashstring","flag")
      val incorrectflagdf = dfinserts1.union(dftobeappendedfiltered)
      val trueflagdf = incorrectflagdf.filter(col("flag")===true)
      val w = Window.partitionBy(col("emp_id")).orderBy(col("updated_at").desc)
      val refined_df = trueflagdf.withColumn("rn", row_number().over(w)).where(col("rn") === 1).select("emp_id","ofc_location","updated_at","hashstring")//select all cols except rn
        .toDF("emp_idu","ofc_locationu","updated_atu","hashstringu")
      val newdf = incorrectflagdf.join(refined_df, refined_df("emp_idu")===incorrectflagdf("emp_id"), "leftouter")
      val finalupdateddf = newdf.withColumn("flag", when(col("emp_idu").isNotNull && col("hashstring")=!= col("hashstringu"), false).otherwise(col("flag")) )
        .select("emp_id","ofc_location","updated_at","hashstring","flag")

      val dfdelete = df1
        .filter(col("Op")==='D')
        .select("emp_id","ofc_location","updated_at")
        .withColumn("hashstring", md5(concat(col("ofc_location"),col("updated_at"))))
        .toDF("emp_idd","ofc_locationd","updated_atd","hash_stringd")
      if (!dfdelete.take(1).isEmpty) {
        val final_df_after_Del = finalupdateddf.join(dfdelete, col("emp_id")===col("emp_idd") && col("hashstring")===col("hash_stringd"),"leftouter")
          .withColumn("flag", when(col("emp_idd").isNotNull, false).otherwise(col("flag")))
          .select("emp_id","ofc_location","updated_at","hashstring","flag")
        final_df_after_Del.show()
        //final_df.show()
        final_df_after_Del.write.format("jdbc")

          .option("url", jdbcUrl)
          .option("dbtable", "public.testCode")
          .option("user", "postgres")
          .option("password", "9473249664")
          .mode("overwrite")
          .save()

      }
      else{
        finalupdateddf.write.format("jdbc")

          .option("url", jdbcUrl)
          .option("dbtable", "public.testCode")
          .option("user", "postgres")
          .option("password", "9473249664")
          .mode("overwrite")
          .save()

      }


      val cnt = df1.count()
      val url = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"
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
      val df1 = spark.read.format("csv").option("header","true")
        .option("inferSchema","true")
        .load("s3a://twitter-khyber1/test-code/"+lastfileRead+"/*.csv")
      println("I am inside lastfileRead is not null and lastfileRead != lastfileAdded")
      val cnt = df1.count()
      val lastcount = lastCountFolder


      println(cnt)
      if (cnt != lastcount){
        val pgConnectionProperties = new Properties()
        pgConnectionProperties.put("user","postgres")
        pgConnectionProperties.put("password","9473249664")

        val pgTable = "public.testcode"
        val jdbcUrl = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"



        val dftable  = spark.read.jdbc("jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres", "public.testcode",pgConnectionProperties).cache()
          .select("emp_id","ofc_location","updated_at","hashstring","flag")







        val dfinserts = df1
          .select("emp_id","ofc_location","updated_at")
          .filter(col("Op")==='I' or col("Op")==='i')
          .withColumn("hashstring", md5(concat(col("ofc_location"), col("updated_at"))))
          .withColumn("flag",lit(true))
          .toDF("emp_idi","ofc_locationi","updated_ati","hashstringi","flagi")
        val tobeinserteddf = dfinserts.join(dftable, dfinserts("emp_idi")===dftable("emp_id") && dfinserts("hashstringi")===dftable("hashstring"), "leftouter")
          .filter(col("emp_id").isNull).select("emp_idi","ofc_locationi","updated_ati","hashstringi","flagi")
        val renameddf = tobeinserteddf.toDF("emp_id","ofc_location","updated_at","hashstring","flag")
        val afterinsertdf = dftable.union(renameddf)
        println("appended newly inserts")
        afterinsertdf.show()
        val dfinserts1 = afterinsertdf
        dfinserts1.show()
        //
        val dfupdate = df1
          .filter(col("Op")==='U')
          .select("emp_id","ofc_location","updated_at")
          .withColumn("hashstring", md5(concat(col("ofc_location"), col("updated_at"))))
          .withColumn("flag",lit(true))
          .toDF("emp_idu","ofc_locationu","updated_atu","hashstringu","flag")
        //
        val dftobeappendedunfiltered = dfupdate.join(dfinserts1,dfinserts1("emp_id")===dfupdate("emp_idu") && dfinserts1("hashstring")===dfupdate("hashstringu"),"leftouter")
        val dftobeappendedfiltered = dftobeappendedunfiltered.filter(col("emp_id").isNull).select("emp_idu","ofc_locationu","updated_atu","hashstringu")
          .withColumn("flag",lit(true)).toDF("emp_id","ofc_location","updated_at","hashstring","flag")
        val incorrectflagdf = dfinserts1.union(dftobeappendedfiltered)
        val trueflagdf = incorrectflagdf.filter(col("flag")===true)
        val w = Window.partitionBy(col("emp_id")).orderBy(col("updated_at").desc)
        val refined_df = trueflagdf.withColumn("rn", row_number().over(w)).where(col("rn") === 1).select("emp_id","ofc_location","updated_at","hashstring")//select all cols except rn
          .toDF("emp_idu","ofc_locationu","updated_atu","hashstringu")
        val newdf = incorrectflagdf.join(refined_df, refined_df("emp_idu")===incorrectflagdf("emp_id"), "leftouter")
        val finalupdateddf = newdf.withColumn("flag", when(col("emp_idu").isNotNull && col("hashstring")=!= col("hashstringu"), false).otherwise(col("flag")) )
          .select("emp_id","ofc_location","updated_at","hashstring","flag")

        val dfdelete = df1
          .filter(col("Op")==='D')
          .select("emp_id","ofc_location","updated_at")
          .withColumn("hashstring", md5(concat(col("ofc_location"),col("updated_at"))))
          .toDF("emp_idd","ofc_locationd","updated_atd","hash_stringd")
        if (!dfdelete.take(1).isEmpty) {
          val final_df_after_Del = finalupdateddf.join(dfdelete, col("emp_id")===col("emp_idd") && col("hashstring")===col("hash_stringd"),"leftouter")
            .withColumn("flag", when(col("emp_idd").isNotNull, false).otherwise(col("flag")))
            .select("emp_id","ofc_location","updated_at","hashstring","flag")
          final_df_after_Del.show()
          final_df_after_Del.write.format("jdbc")

            .option("url", jdbcUrl)
            .option("dbtable", "public.testCode")
            .option("user", "postgres")
            .option("password", "9473249664")
            .mode("overwrite")
            .save()

        }
        else{
          finalupdateddf.write.format("jdbc")

            .option("url", jdbcUrl)
            .option("dbtable", "public.testCode")
            .option("user", "postgres")
            .option("password", "9473249664")
            .mode("overwrite")
            .save()

        }


        val cnt = df1.count()
        val url = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"
        val conn = DriverManager.getConnection(url,"postgres","9473249664")

        try {
          val stm = conn.createStatement()
          println("Updating auditread")

          val rs = stm.executeUpdate("UPDATE public.AuditReadWrite SET lastfileread = lastfileadded,lastcountfolder=" +  cnt)


        } finally {
          conn.close()
        }








      }
    }
  }

  else{

    val df1 = spark.read.format("csv").option("header","true")
      .option("inferSchema","true")
      .load("s3a://twitter-khyber1/test-code/"+lastfileRead+"/*.csv")


    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","9473249664")

    val pgTable = "public.testcode"
    val jdbcUrl = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"



    val dftable  = spark.read.jdbc("jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres", "public.testcode",pgConnectionProperties).cache()
      .select("emp_id","ofc_location","updated_at","hashstring","flag")







    val dfinserts = df1
      .select("emp_id","ofc_location","updated_at")
      .filter(col("Op")==='I' or col("Op")==='i')
      .withColumn("hashstring", md5(concat(col("ofc_location"), col("updated_at"))))
      .withColumn("flag",lit(true))
      .toDF("emp_idi","ofc_locationi","updated_ati","hashstringi","flagi")
    val tobeinserteddf = dfinserts.join(dftable, dfinserts("emp_idi")===dftable("emp_id") && dfinserts("hashstringi")===dftable("hashstring"), "leftouter")
      .filter(col("emp_id").isNull).select("emp_idi","ofc_locationi","updated_ati","hashstringi","flagi")
    val renameddf = tobeinserteddf.toDF("emp_id","ofc_location","updated_at","hashstring","flag")
    val afterinsertdf = dftable.union(renameddf)
    println("appended newly inserts")
    afterinsertdf.show()
    val dfinserts1 = afterinsertdf
    dfinserts1.show()
    //
    val dfupdate = df1
      .filter(col("Op")==='U')
      .select("emp_id","ofc_location","updated_at")
      .withColumn("hashstring", md5(concat(col("ofc_location"), col("updated_at"))))
      .withColumn("flag",lit(true))
      .toDF("emp_idu","ofc_locationu","updated_atu","hashstringu","flag")
    //
    val dftobeappendedunfiltered = dfupdate.join(dfinserts1,dfinserts1("emp_id")===dfupdate("emp_idu") && dfinserts1("hashstring")===dfupdate("hashstringu"),"leftouter")
    val dftobeappendedfiltered = dftobeappendedunfiltered.filter(col("emp_id").isNull).select("emp_idu","ofc_locationu","updated_atu","hashstringu")
      .withColumn("flag",lit(true)).toDF("emp_id","ofc_location","updated_at","hashstring","flag")
    val incorrectflagdf = dfinserts1.union(dftobeappendedfiltered)
    val trueflagdf = incorrectflagdf.filter(col("flag")===true)
    val w = Window.partitionBy(col("emp_id")).orderBy(col("updated_at").desc)
    val refined_df = trueflagdf.withColumn("rn", row_number().over(w)).where(col("rn") === 1).select("emp_id","ofc_location","updated_at","hashstring")//select all cols except rn
      .toDF("emp_idu","ofc_locationu","updated_atu","hashstringu")
    val newdf = incorrectflagdf.join(refined_df, refined_df("emp_idu")===incorrectflagdf("emp_id"), "leftouter")
    val finalupdateddf = newdf.withColumn("flag", when(col("emp_idu").isNotNull && col("hashstring")=!= col("hashstringu"), false).otherwise(col("flag")) )
      .select("emp_id","ofc_location","updated_at","hashstring","flag")

    val dfdelete = df1
      .filter(col("Op")==='D')
      .select("emp_id","ofc_location","updated_at")
      .withColumn("hashstring", md5(concat(col("ofc_location"),col("updated_at"))))
      .toDF("emp_idd","ofc_locationd","updated_atd","hash_stringd")
    if (!dfdelete.take(1).isEmpty) {
      val final_df_after_Del = finalupdateddf.join(dfdelete, col("emp_id")===col("emp_idd") && col("hashstring")===col("hash_stringd"),"leftouter")
        .withColumn("flag", when(col("emp_idd").isNotNull, false).otherwise(col("flag")))
        .select("emp_id","ofc_location","updated_at","hashstring","flag")
      final_df_after_Del.show()
      final_df_after_Del.write.format("jdbc")

        .option("url", jdbcUrl)
        .option("dbtable", "public.testCode")
        .option("user", "postgres")
        .option("password", "9473249664")
        .mode("overwrite")
        .save()

    }
    else{
      finalupdateddf.write.format("jdbc")

        .option("url", jdbcUrl)
        .option("dbtable", "public.testCode")
        .option("user", "postgres")
        .option("password", "9473249664")
        .mode("overwrite")
        .save()

    }


    val cnt = df1.count()
    val url = "jdbc:postgresql://database-1.c0wamwj4fyvi.ap-northeast-1.rds.amazonaws.com:5432/postgres"
    val conn = DriverManager.getConnection(url,"postgres","9473249664")

    try {
      val stm = conn.createStatement()
      println("Updating auditread")

      val rs = stm.executeUpdate("UPDATE public.AuditReadWrite SET lastfileread = lastfileadded,lastcountfolder=" +  cnt)


    } finally {
      conn.close()
    }


  }
}
