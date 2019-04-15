/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.tags

import scala.collection.immutable
import java.lang.{ Integer => JInt, Long => JLong }
import java.net.URLEncoder
import java.util.UUID

import akka.Done
import akka.pattern.ask
import akka.pattern.pipe
import akka.actor.{ Actor, ActorLogging, ActorRef, NoSerializationVerificationNeeded, Props }
import akka.annotation.InternalApi
import akka.util.Timeout
import com.datastax.driver.core.{ BatchStatement, PreparedStatement, ResultSet, Statement }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import akka.actor.Timers
import akka.util.ByteString
import akka.persistence.tags.TagWriter._

private[akka] case class TagWritersSession(
  tagWritePs: Future[PreparedStatement],
  executeStatement: Statement => Future[Done],
  selectStatement: Statement => Future[ResultSet],
  tagProgressPs: Future[PreparedStatement],
  tagScanningPs: Future[PreparedStatement]) {

  import TagWriters._

  def writeBatch(tag: Tag, events: Seq[(CassandraTagSerialized, Long)])(implicit ec: ExecutionContext): Future[Done] = {
    val batch = new BatchStatement(BatchStatement.Type.UNLOGGED)

    tagWritePs.map { withoutMeta =>
      events.foreach {
        case (event, pidTagSequenceNr) => {

          val ps = withoutMeta
          val bound = ps.bind(
            tag,
            event.timeBucket.key: JLong,
            event.timeUuid,
            pidTagSequenceNr: JLong,
            event.tagSerialized.serialized,
            event.tagSerialized.eventAdapterManifest,
            event.persistenceId,
            event.sequenceNr: JLong,
            event.tagSerialized.serId: JInt,
            event.tagSerialized.serManifest,
            event.tagSerialized.writerUuid)
          batch.add(bound)
        }
      }
      batch
    }.flatMap(executeStatement)
  }

  def writeProgress(tag: Tag, persistenceId: String, seqNr: Long, tagPidSequenceNr: Long, offset: UUID)(implicit ec: ExecutionContext): Future[Done] = {
    tagProgressPs.map(ps =>
      ps.bind(persistenceId, tag, seqNr: JLong, tagPidSequenceNr: JLong, offset)).flatMap(executeStatement)
  }

}

object TagWriters {

  type Tag = String
  type PersistenceId = String
  type SequenceNr = Long
  type TagPidSequenceNr = Long

  /**
   * All tag writes should be for the same persistenceId
   */
  case class CassandraBulkTagWrite(tagWrites: immutable.Seq[CassandraTagWrite], withoutTags: immutable.Seq[CassandraTagSerialized])
    extends NoSerializationVerificationNeeded

  /**
   * All serialised should be for the same persistenceId
   */
  private[akka] case class CassandraTagWrite(tag: Tag, serialised: immutable.Seq[CassandraTagSerialized])
    extends NoSerializationVerificationNeeded

  def props(settings: TagWriterSettings, tagWriterSession: TagWritersSession): Props =
    Props(new TagWriters(settings, tagWriterSession))

  final case class TagFlush(tag: String)

  case object FlushAllTagWriters

  case object AllFlushed

  final case class PersistentActorStarting(pid: String, tagProgresses: Map[Tag, TagProgress], persistentActor: ActorRef)

  case object PersistentActorStartingAck

  final case class TagWriteFailed(reason: Throwable)

  private case object WriteTagScanningTick

  private case class PersistentActorTerminated(pid: PersistenceId, ref: ActorRef)

}

/**
 * Manages all the tag writers.
 */
@InternalApi private[akka] class TagWriters(settings: TagWriterSettings, tagWriterSession: TagWritersSession)
  extends Actor with Timers with ActorLogging {

  import TagWriters._

  import context.dispatcher

  // eager init and val because used from Future callbacks
  override val log = super.log

  private var tagActors = Map.empty[String, ActorRef]
  // just used for local actor asks
  private implicit val timeout = Timeout(10.seconds)

  private var toBeWrittenScanning: Map[PersistenceId, SequenceNr] = Map.empty
  private var pendingScanning: Map[PersistenceId, SequenceNr] = Map.empty

  private var currentPersistentActors: Map[PersistenceId, ActorRef] = Map.empty

  timers.startPeriodicTimer(WriteTagScanningTick, WriteTagScanningTick, settings.scanningFlushInterval)

  def receive: Receive = {
    case FlushAllTagWriters =>
      log.debug("Flushing all tag writers")
      // will include a C* write so be patient
      implicit val timeout = Timeout(10.seconds)
      val replyTo = sender()
      val flushes = tagActors.map { case (_, ref) => (ref ? Flush).mapTo[FlushComplete.type] }
      Future.sequence(flushes).map(_ => AllFlushed) pipeTo replyTo
    case TagFlush(tag) =>
      tagActor(tag).tell(Flush, sender())
    case tw: CassandraTagWrite =>
      updatePendingScanning(tw.serialised)
      tagActor(tw.tag) forward tw
    case CassandraBulkTagWrite(tws, withoutTags) =>
      tws.foreach { tw =>
        updatePendingScanning(tw.serialised)
        tagActor(tw.tag) forward tw
      }
      updatePendingScanning(withoutTags)
    case WriteTagScanningTick =>
      writeTagScanning()

    case PersistentActorStarting(pid, tagProgresses: Map[Tag, TagProgress], persistentActor) =>
      val missingProgress = tagActors.keySet -- tagProgresses.keySet
      log.debug("Persistent actor [{}] with pid [{}] starting with progress [{}]. Tags to reset as not in progress: [{}]", persistentActor, pid, tagProgresses, missingProgress)

      // EventsByTagMigration uses dead letters are there are no real actors
      if (persistentActor != context.system.deadLetters) {
        currentPersistentActors += (pid -> persistentActor)
        context.watchWith(persistentActor, PersistentActorTerminated(pid, persistentActor))
      }

      val replyTo = sender()
      pendingScanning -= pid
      val tagWriterAcks = Future.sequence(tagProgresses.map {
        case (tag, progress) =>
          log.debug("Sending tag progress: [{}] [{}]", tag, progress)
          (tagActor(tag) ? ResetPersistenceId(tag, progress)).mapTo[ResetPersistenceIdComplete.type]
      })
      // We send an empty progress in case the tag actor has buffered events
      // and has never written any tag progress for this tag/pid
      val blankTagWriterAcks = Future.sequence(missingProgress.map { tag =>
        log.debug("Sending blank progress for tag [{}] pid [{}]", tag, pid)
        (tagActor(tag) ? ResetPersistenceId(tag, TagProgress(pid, 0, 0))).mapTo[ResetPersistenceIdComplete.type]
      })

      val recoveryNotificationComplete = for {
        _ <- tagWriterAcks
        _ <- blankTagWriterAcks
      } yield Done

      // if this fails (all local actor asks) the recovery will timeout
      recoveryNotificationComplete.foreach { _ => replyTo ! PersistentActorStartingAck }

    case TagWriteFailed(_) =>
      toBeWrittenScanning = Map.empty
      pendingScanning = Map.empty

    case PersistentActorTerminated(pid, ref) =>
      currentPersistentActors.get(pid) match {
        case Some(currentRef) if currentRef == ref =>
          log.debug("Persistent actor terminated [{}]. Informing TagWriter actors to drop state for pid: [{}]", ref, pid)
          tagActors.foreach {
            case (_, tagWriterRef) => tagWriterRef ! DropState(pid)
          }
          currentPersistentActors -= pid
        case Some(currentRef) =>
          log.debug("Persistent actor terminated. However new actor ref for pid has been added. [{}]. Terminated ref: [{}] terminatedRef: [{}]", pid, ref, currentRef)
        case None =>
          log.warning("Unknown persistent actor terminated. Please raise an issue with debug logs. Pid: [{}]. Ref: [{}]", pid, ref)
      }
  }

  private def updatePendingScanning(serialized: immutable.Seq[CassandraTagSerialized]): Unit = {
    serialized.foreach { ser =>
      pendingScanning.get(ser.persistenceId) match {
        case Some(seqNr) =>
          if (ser.sequenceNr > seqNr) // collect highest
            pendingScanning = pendingScanning.updated(ser.persistenceId, ser.sequenceNr)
        case None =>
          pendingScanning = pendingScanning.updated(ser.persistenceId, ser.sequenceNr)
      }
    }
  }

  private def writeTagScanning(): Unit = {
    val updates = toBeWrittenScanning
    // current pendingScanning will be written on next tick, if no write failures
    toBeWrittenScanning = pendingScanning
    // collect new until next tick
    pendingScanning = Map.empty

    if (updates.nonEmpty && log.isDebugEnabled)
      log.debug("Update tag scanning [{}]", updates.toSeq.mkString(","))

    tagWriterSession.tagScanningPs.foreach { ps =>
      updates.foreach {
        case (pid, seqNr) =>
          tagWriterSession.executeStatement(ps.bind(pid, seqNr: JLong))
            .failed.foreach { t =>
              log.warning("Writing tag scanning failed. Reason {}", t)
              self ! TagWriteFailed(t)
            }
      }
    }
  }

  private def tagActor(tag: String): ActorRef =
    tagActors.get(tag) match {
      case None =>
        val ref = createTagWriter(tag)
        tagActors += (tag -> ref)
        ref
      case Some(ar) => ar
    }

  // protected for testing purposes
  protected def createTagWriter(tag: String): ActorRef = {
    context.actorOf(
      TagWriter.props(settings, tagWriterSession, tag)
        .withDispatcher(context.props.dispatcher),
      name = URLEncoder.encode(tag, ByteString.UTF_8))
  }

}
