/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.Done
import akka.persistence.PersistentRepr
import akka.persistence.query.{ EventEnvelope, NoOffset }
import akka.stream.testkit.scaladsl.TestSink

import scala.concurrent.Await

class EventsByTagMigrationLoadSpec extends AbstractEventsByTagMigrationSpec {

  val NrPersistenceIds = 100
  val NrEventsPerPersistenceId = 1000

  def time(desc: String, f: () => Unit): Unit = {
    val startTime = System.nanoTime()
    f()
    val endTime = System.nanoTime()
    import scala.concurrent.duration._
    import akka.util.PrettyDuration._
    println(s"$desc: " + (endTime - startTime).nanos.pretty)
  }

  "EventsByTagMigration" should {
    "work" in {

      time("Writing old events", () => {
        for (p <- 1 to NrPersistenceIds) {
          for (e <- 1 to NrEventsPerPersistenceId) {
            writeOldTestEventWithTags(PersistentRepr(s"e-${e}", e, s"p-$p"), Set("blue", "red"))
          }
        }
      })

      migrator.createTables().futureValue shouldEqual Done
      migrator.addTagsColumn().futureValue shouldEqual Done

      // since we are writing the events directly the all_persistence_ids table must also be updated
      reconciler.rebuildAllPersistenceIds().futureValue

      import scala.concurrent.duration._
      time("Migration", () => {
        Await.result(migrator.migrateToTagViews(), 10.minutes)
      })

      val blueSrc = queries.eventsByTag("blue", NoOffset)
      val blueProbe = blueSrc.runWith(TestSink.probe[Any])
      val redSrc = queries.eventsByTag("red", NoOffset)
      val redProbe = redSrc.runWith(TestSink.probe[Any])
      for (p <- 1 to NrPersistenceIds) {
        val expectedPid = s"p-$p"
        for (e <- 1 to NrEventsPerPersistenceId) {
          val expectedEvent = s"e-$e"
          blueProbe.request(1)
          system.log.debug(s"Expecting $expectedPid $expectedEvent")
          blueProbe.expectNextPF { case EventEnvelope(_, `expectedPid`, e, `expectedEvent`) => }
          redProbe.request(1)
          redProbe.expectNextPF { case EventEnvelope(_, `expectedPid`, e, `expectedEvent`) => }
        }
      }
      blueProbe.cancel()
      redProbe.cancel()

    }
  }

// don't clean c* up
//  override protected def afterAll(): Unit = {
//  }
}
