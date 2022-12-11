package scala

import com.typesafe.config.ConfigFactory

import java.io.File



object readjson extends App {
 // val config = ConfigFactory.parseFile("/home/prasoon/Downloads/scala-Practice/src/main/scala/scala/application.conf")
 val config1 = ConfigFactory.parseFile(new File("/home/prasoon/Downloads/scala-Practice/src/main/scala/scala/application.conf"))
  println(config1)
    //.getConfig("from-db-schema-table")
  val targetdetails = config1.getConfig("from-db-schema-table").getConfig("target")
  println(targetdetails)
  println(targetdetails.getString("jdbcurl"))
}
