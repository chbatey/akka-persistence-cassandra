package akka.persistence

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID

import com.datastax.driver.core.utils.UUIDs

package object tags {
  private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS")

  def formatOffset(uuid: UUID): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(UUIDs.unixTimestamp(uuid)), ZoneOffset.UTC)
    s"$uuid (${timestampFormatter.format(time)})"
  }

  def formatUnixTime(unixTime: Long): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTime), ZoneOffset.UTC)
    timestampFormatter.format(time)
  }
}
