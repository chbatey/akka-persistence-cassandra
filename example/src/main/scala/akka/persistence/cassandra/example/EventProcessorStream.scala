package akka.persistence.cassandra.example

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery, TimeBasedUUID }
import akka.persistence.typed.PersistenceId
import akka.stream.SharedKillSwitch
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.{ RestartSource, Sink, Source }
import akka.{ Done, NotUsed }
import com.datastax.oss.driver.api.core.cql.Row
import org.HdrHistogram.Histogram
import org.slf4j.{ Logger, LoggerFactory }

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

class EventProcessorStream[Event: ClassTag](
    system: ActorSystem[_],
    executionContext: ExecutionContext,
    eventProcessorId: String,
    tag: String) {

  protected val log: Logger = LoggerFactory.getLogger(getClass)
  implicit val sys: ActorSystem[_] = system
  implicit val ec: ExecutionContext = executionContext

  private val session = CassandraSessionRegistry(system).sessionFor("akka.persistence.cassandra")

  private val query = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

  def runQueryStream(killSwitch: SharedKillSwitch, histogram: Histogram): Unit = {
    RestartSource
      .withBackoff(minBackoff = 500.millis, maxBackoff = 20.seconds, randomFactor = 0.1) { () =>
        Source.futureSource {
          readOffset().map { offset =>
            log.infoN("Starting stream for tag [{}] from offset [{}]", tag, offset)
            processEventsByTag(offset, histogram)
            // groupedWithin can be used here to improve performance by reducing number of offset writes,
            // with the trade-off of possibility of more duplicate events when stream is restarted
              .mapAsync(1)(writeOffset)
          }
        }
      }
      .via(killSwitch.flow)
      .runWith(Sink.ignore)
  }

  private def processEventsByTag(offset: Offset, histogram: Histogram): Source[Offset, NotUsed] = {
    query
      .eventsByTag(tag, offset)
      .statefulMapConcat { () =>
        val previous = mutable.HashMap[String, EventEnvelope]()
        var counter = 0L

        { ee =>
          counter += 1
          previous.get(ee.persistenceId) match {
            case None => // first time we se e this pid
            case Some(prev) =>
              if (prev.sequenceNr != ee.sequenceNr - 1)
                log.errorN("Out or order sequence nr. Previous [{}]. Current [{}]", prev, ee)
          }
          previous.put(ee.persistenceId, ee)
          if (counter % 100 == 0) log.info("Successfully processed [{}] events for tag [{}]", counter, tag)
          ee :: Nil
        }
      }
      .mapAsync(1) { eventEnvelope =>
        eventEnvelope.event match {
          case event: Event => {
            // Times from different nodes, take with a pinch of salt
            val latency = System.currentTimeMillis() - eventEnvelope.timestamp
            // when restarting without the offset the latency will be too big
            if (latency < histogram.getMaxValue) {
              histogram.recordValue(latency)
            } else {
              log.info("Dropping latency as it is too large {}", latency)
            }
            log.debugN(
              "Tag {} Event {} persistenceId {}, sequenceNr {}. Latency {}",
              tag,
              event,
              PersistenceId.ofUniqueId(eventEnvelope.persistenceId),
              eventEnvelope.sequenceNr,
              latency)
            Future.successful(Done)
          }.map(_ => eventEnvelope.offset)
          case other =>
            Future.failed(new IllegalArgumentException(s"Unexpected event [${other.getClass.getName}]"))
        }
      }
  }

  private def readOffset(): Future[Offset] = {
    session
      .selectOne(
        "SELECT timeUuidOffset FROM akka.offsetStore WHERE eventProcessorId = ? AND tag = ?",
        eventProcessorId,
        tag)
      .map(extractOffset)
  }

  private def extractOffset(maybeRow: Option[Row]): Offset = {
    maybeRow match {
      case Some(row) =>
        val uuid = row.getUuid("timeUuidOffset")
        if (uuid == null) {
          startOffset()
        } else {
          TimeBasedUUID(uuid)
        }
      case None => startOffset()
    }
  }

  // start looking from one week back if no offset was stored
  private def startOffset(): Offset = {
    query.timeBasedUUIDFrom(System.currentTimeMillis() - (7 * 24 * 60 * 60 * 1000))
  }

  private def writeOffset(offset: Offset)(implicit ec: ExecutionContext): Future[Done] = {
    offset match {
      case t: TimeBasedUUID =>
        session.executeWrite(
          "INSERT INTO akka.offsetStore (eventProcessorId, tag, timeUuidOffset) VALUES (?, ?, ?)",
          eventProcessorId,
          tag,
          t.value)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected offset type $offset")
    }

  }

}
