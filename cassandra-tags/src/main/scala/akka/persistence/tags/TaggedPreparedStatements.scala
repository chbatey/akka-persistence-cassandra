/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.tags

import akka.Done
import akka.cassandra.common.TableSettings
import akka.cassandra.session.scaladsl.CassandraSession
import com.datastax.driver.core.{PreparedStatement, Session}

import scala.concurrent.{ExecutionContext, Future}
import akka.cassandra.session._

private[akka] class TaggedPreparedStatements(ec: ExecutionContext, keyspaceName: String, tagTable: TableSettings) {

  private implicit val executionContext = ec

  private def tagTableName = s"$keyspaceName.tag_views"
  private def tagProgressTableName = s"$keyspaceName.tag_progress"
  private def tagScanningTableName = s"$keyspaceName.tag_scanning"

  def createTables(session: Session): Future[Done] = {

    for {
      _ <- session.executeAsync(createTagsTable).asScala
      _ <- session.executeAsync(createTagsProgressTable).asScala
      _ <- session.executeAsync(createTagScanningTable).asScala
    } yield Done

  }

  private[akka] def writeTagProgress =
    s"""
       INSERT INTO $tagProgressTableName(
        persistence_id,
        tag,
        sequence_nr,
        tag_pid_sequence_nr,
        offset) VALUES (?, ?, ?, ?, ?)
     """

  private[akka] def selectTagScanningForPersistenceId =
    s"""
       SELECT sequence_nr from $tagScanningTableName WHERE
       persistence_id = ?
     """

  private[akka] def writeTags(withMeta: Boolean) =
    s"""
       INSERT INTO $tagTableName(
        tag_name,
        timebucket,
        timestamp,
        tag_pid_sequence_nr,
        event,
        event_manifest,
        persistence_id,
        sequence_nr,
        ser_id,
        ser_manifest,
        writer_uuid
        ${if (withMeta) ", meta_ser_id, meta_ser_manifest, meta" else ""}
        ) VALUES (?,?,?,?,?,?,?,?,?,?,
        ${if (withMeta) "?, ?, ?," else ""}
        ?)
     """

  private[akka] def selectTagProgress =
    s"""
       SELECT * from $tagProgressTableName WHERE
       persistence_id = ? AND
       tag = ?
     """

  private[akka] def selectTagProgressForPersistenceId =
    s"""
       SELECT * from $tagProgressTableName WHERE
       persistence_id = ?
     """

  private[akka] def writeTagScanning =
    s"""
       INSERT INTO $tagScanningTableName(
         persistence_id, sequence_nr) VALUES (?, ?)
     """

  // FIXME, include in the output of the schema
 def createTagsTable =
    s"""
      |CREATE TABLE IF NOT EXISTS ${tagTableName} (
      |  tag_name text,
      |  persistence_id text,
      |  sequence_nr bigint,
      |  timebucket bigint,
      |  timestamp timeuuid,
      |  tag_pid_sequence_nr bigint,
      |  writer_uuid text,
      |  ser_id int,
      |  ser_manifest text,
      |  event_manifest text,
      |  event blob,
      |  meta_ser_id int,
      |  meta_ser_manifest text,
      |  meta blob,
      |  PRIMARY KEY ((tag_name, timebucket), timestamp, persistence_id, tag_pid_sequence_nr))
      |  WITH gc_grace_seconds =${tagTable.gcGraceSeconds}
      |  AND compaction = ${tagTable.compactionStrategy.asCQL}
      |  ${if (tagTable.ttl.isDefined) "AND default_time_to_live = " + tagTable.ttl.get.toSeconds else ""}
    """.stripMargin.trim

  def createTagsProgressTable =
    s"""
     |CREATE TABLE IF NOT EXISTS $tagProgressTableName(
     |  persistence_id text,
     |  tag text,
     |  sequence_nr bigint,
     |  tag_pid_sequence_nr bigint,
     |  offset timeuuid,
     |  PRIMARY KEY (persistence_id, tag))
     """.stripMargin.trim

  def createTagScanningTable =
    s"""
     |CREATE TABLE IF NOT EXISTS $tagScanningTableName(
     |  persistence_id text,
     |  sequence_nr bigint,
     |  PRIMARY KEY (persistence_id))
     """.stripMargin.trim



}
