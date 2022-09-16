package scala

import java.sql.{DriverManager, ResultSet}
import scala.DataWarehouse.url

object PostgresWIthScala extends App{

  val url = "jdbc:postgresql://database-1.c6vrj3gbkajn.us-east-1.rds.amazonaws.com:5432/postgres"
  val con_str = url+"?user=postgres"
  val conn = DriverManager.getConnection(url,"postgres","9473249664")
  try {
    val stm = conn.createStatement()
  val cnt =25
    val rs = stm.executeUpdate("UPDATE public.AuditReadWrite SET lastfileread = lastfileadded, lastcountfolder ="+ cnt +"WHERE lastfileadded = 20220807")


  } finally {
    conn.close()
  }


}
