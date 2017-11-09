package akka.persistence.cassandra


import akka.actor.{ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.cassandra.CassandraEventsByTagLoadSpec.TaggingActor
import akka.persistence.cassandra.CassandraEventsByTagLoadSpec.TaggingActor.Ack
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.journal.Tagged
import akka.persistence.query.{NoOffset, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.{ImplicitSender, TestKit}
import com.datastax.driver.core.Cluster
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Matchers, WordSpecLike}
import scala.concurrent.duration._

import scala.util.Try

object CassandraEventsByTagLoadSpec {

  val keyspaceName = "CassandraEventsByTagLoadSpec"

  val config = ConfigFactory.parseString(
    s"""
       |akka {
       |  loglevel = INFO
       |}
       |cassandra-journal {
       |  keyspace = $keyspaceName
       |  log-queries = off
       |
       |  tags {
       |      orange = 1
       |      green = 2
       |      red = 3
       |   }
       |}
       |cassandra-snapshot-store.keyspace=CassandraEventsByTagLoadSpecSnapshot
       |cassandra-query-journal = {
       |  first-time-bucket = "20171105"
       |}
       |akka.actor.serialize-messages=off
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)


  object TaggingActor {
    case object Ack
  }

  class TaggingActor(val persistenceId: String, tags: Set[String]) extends PersistentActor {
    def receiveRecover: Receive = processEvent

    def receiveCommand: Receive = {
      case event: String =>
        persist(Tagged(event, tags)) { e =>
          processEvent(e)
          sender() ! Ack
        }
    }

    def processEvent: Receive = {
      case _ =>
    }
  }
}

class CassandraEventsByTagLoadSpec extends TestKit(ActorSystem("CassandraEventsByTagLoadSpec", CassandraEventsByTagLoadSpec.config))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with CassandraLifecycle
  with ScalaFutures {

  override def systemName = "CassandraEventsByTagLoadSpec"

  implicit val materialiser = ActorMaterializer()(system)
  implicit override val patienceConfig = PatienceConfig(timeout = Span(60, Seconds), interval = Span(5, Seconds))

  val nrPersistenceIds = 100
  val eventTags = Set("orange", "green", "red")
  val messagesPerPersistenceId = 1000
  val veryLongWait = 60.seconds

  "Events by tag" must {
    "Events should come in sequence number order for each persistence id" in {
      val refs: Vector[ActorRef] = (1 to nrPersistenceIds).map(i => {
        system.actorOf(Props(classOf[TaggingActor], s"p-$i", eventTags))
      }).toVector

      1L to messagesPerPersistenceId foreach { i =>
        refs.foreach { ref =>
          ref ! s"e-$i"
        }
      }

      val readJournal =
        PersistenceQuery(system).readJournalFor[CassandraReadJournal]("cassandra-query-journal")

      eventTags.foreach({ tag =>
        try {
          validateTagStream(readJournal)(tag)
        } catch {
          case e: Throwable =>
            system.log.error("IMPORTANT:: Failed, retrying to see if it was eventual consistency")
            system.log.error("IMPORTANT:: " + e.getMessage)
            validateTagStream(readJournal)(tag)
            system.log.info("IMPORTANT:: Passed second time")
            throw new RuntimeException("Only passed the second time")
        }

      })
    }
  }

  private def validateTagStream(readJournal: CassandraReadJournal)(tag: String): Unit = {
    system.log.info(s"Validating tag $tag")
    val probe = readJournal.eventsByTag("orange", NoOffset).toMat(TestSink.probe)(Keep.right).run
    var sequenceNrsPerPid = Map[String, Long]()
    var allReceived = Map.empty[String, List[Long]].withDefaultValue(List.empty[Long])
    probe.request(messagesPerPersistenceId * nrPersistenceIds)

    (1 to (messagesPerPersistenceId * nrPersistenceIds)) foreach { i: Int =>
      val event = try {
        probe.expectNext(veryLongWait)
      } catch {
        case e: AssertionError =>
          system.log.error(e, "Failed to get event")
          allReceived.filter(_._2.size != messagesPerPersistenceId).foreach(p => system.log.info("{}", p))
          throw e
      }

      allReceived += (event.persistenceId -> (event.sequenceNr :: allReceived(event.persistenceId)))
      var fail = false
      sequenceNrsPerPid.get(event.persistenceId) match {
        case Some(currentSeqNr) =>
          withClue(s"Out of order sequence nrs for pid ${event.persistenceId}. This was event nr [$i]") {
            if (event.sequenceNr != currentSeqNr + 1) {
              fail = true
              println("IMPORTANT:: " + s"Out of order sequence nrs for pid ${event.persistenceId}. This was event nr [$i]. Expected ${currentSeqNr + 1}, got: ${event.sequenceNr}")
            }
            //            event.sequenceNr should equal(currentSeqNr + 1)
          }
          sequenceNrsPerPid += (event.persistenceId -> event.sequenceNr)
        case None =>
          event.sequenceNr should equal(1)
          sequenceNrsPerPid += (event.persistenceId -> event.sequenceNr)
      }
    }
  }

  override protected def externalCassandraCleanup(): Unit = {
    val cluster = Cluster.builder()
      .addContactPoint("localhost")
      .build()
    Try(cluster.connect().execute(s"drop keyspace ${CassandraEventsByTagLoadSpec.keyspaceName}"))
    cluster.close()
  }
}
