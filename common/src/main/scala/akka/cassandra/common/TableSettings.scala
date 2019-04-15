package akka.cassandra.common
import akka.cassandra.common.compaction.CassandraCompactionStrategy

import scala.concurrent.duration.Duration

case class TableSettings(name: String, compactionStrategy: CassandraCompactionStrategy, gcGraceSeconds: Long, ttl: Option[Duration])
