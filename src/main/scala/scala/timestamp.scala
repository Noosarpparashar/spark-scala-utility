package scala
import java.time.format.DateTimeFormatter

object timestamp extends App {
  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter

  val format = "yyyyMMdd_HHmmss"

  val dtf = DateTimeFormatter.ofPattern(format)

  val ldt = LocalDateTime.now()

  println(ldt.format(dtf))


}
