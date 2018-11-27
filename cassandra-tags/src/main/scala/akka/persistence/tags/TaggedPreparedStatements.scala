/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.tags

import akka.cassandra.session.scaladsl.CassandraSession
import com.datastax.driver.core.PreparedStatement

import scala.concurrent.{ExecutionContext, Future}

trait TaggedPreparedStatements {

  private[akka] val session: CassandraSession
  private[akka] implicit val ec: ExecutionContext

  // FIXME use name from config
  private def tagTableName = "akka.tag_views"

  // FIXME use name from config
  private def tagProgressTableName = "akka.tag_progress"

  // FIXME keyspace from config
  private def tagScanningTableName = "akka.tag_scanning"

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

  def preparedWriteToTagViewWithoutMeta: Future[PreparedStatement] = session.prepare(writeTags(false)).map(_.setIdempotent(true))

  def preparedWriteToTagViewWithMeta: Future[PreparedStatement] = session.prepare(writeTags(true)).map(_.setIdempotent(true))

  def preparedWriteToTagProgress: Future[PreparedStatement] = session.prepare(writeTagProgress).map(_.setIdempotent(true))

  def preparedSelectTagProgress: Future[PreparedStatement] = session.prepare(selectTagProgress).map(_.setIdempotent(true))

  def preparedSelectTagProgressForPersistenceId: Future[PreparedStatement] = session.prepare(selectTagProgressForPersistenceId).map(_.setIdempotent(true))

  def preparedWriteTagScanning: Future[PreparedStatement] = session.prepare(writeTagScanning).map(_.setIdempotent(true))

  def preparedSelectTagScanningForPersistenceId: Future[PreparedStatement] = session.prepare(selectTagScanningForPersistenceId).map(_.setIdempotent(true))
}
