package akka.persistence

import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID

import akka.cassandra.common.{Hour, TimeBucket}
import akka.persistence.query.TimeBasedUUID
import akka.persistence.tags.TagWriters.CassandraTagWrite
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

  def toCassandraTagWrite(tw: TagWrite): CassandraTagWrite = {
    CassandraTagWrite(tw.tag, tw.write.map(toCassandraSerialized))
  }

  def toCassandraSerialized(tagSerialized: TagSerialized): CassandraTagSerialized = {
      val offset = tagSerialized.offset match {
        case TimeBasedUUID(uuid) => uuid
        case _ => UUIDs.timeBased()
      }
      // FIXME from config
      val bucket = TimeBucket(offset, Hour)
    CassandraTagSerialized(tagSerialized, offset, bucket)
  }
}
