/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.persistence.PersistentRepr
import akka.persistence.cassandra.query.EventsByPersistenceIdStage.Extractors

import scala.concurrent._

trait CassandraRecovery {
  this: CassandraJournal =>

  private[akka] val config: CassandraJournalConfig

  import config._

  private[akka] val eventDeserializer: CassandraJournal.EventDeserializer =
    new CassandraJournal.EventDeserializer(context.system)

  private[akka] def asyncReadHighestSequenceNrInternal(
    persistenceId:  String,
    fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr {} {}", persistenceId, fromSequenceNr)
    asyncHighestDeletedSequenceNumber(persistenceId).flatMap { h =>
      asyncFindHighestSequenceNr(
        persistenceId,
        math.max(fromSequenceNr, h),
        targetPartitionSize)
    }
  }

  // FIXME, send presnapshot writes, ask tag writer what sequence number it is up to to know which writes to re-send?
  // TODO this serialises and re-serialises the messages for fixing tag_views
  // Could have an events by persistenceId stage that has the raw payload
  override def asyncReplayMessages(
    persistenceId:  String,
    fromSequenceNr: Long,
    toSequenceNr:   Long,
    max:            Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug(
      "asyncReplayMessages pid {} from {} to {}",
      persistenceId,
      fromSequenceNr,
      toSequenceNr)

    queries
      .eventsByPersistenceId(
        persistenceId,
        fromSequenceNr,
        toSequenceNr,
        max,
        replayMaxResultSize,
        None,
        "asyncReplayMessages",
        someReadConsistency,
        someReadRetryPolicy,
        extractor = Extractors.persistentRepr(eventDeserializer, serialization))
      .map(p => queries.mapEvent(p.persistentRepr))
      .runForeach(replayCallback)
      .map(_ => ())
  }

}
