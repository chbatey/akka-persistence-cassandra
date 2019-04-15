package akka.persistence.tags
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, ExtendedActorSystem}
import akka.cassandra.common.compaction.CassandraCompactionStrategy
import akka.cassandra.common.{Hour, TableSettings, TimeBucket}
import akka.cassandra.session.{CassandraSessionSettings, SessionProvider}
import akka.cassandra.session.scaladsl.CassandraSession
import akka.event.Logging
import akka.persistence.query.TimeBasedUUID
import akka.persistence.tags.TagWriters.{CassandraBulkTagWrite, CassandraTagWrite}
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{PreparedStatement, Session}
import com.typesafe.config.Config

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Try

case class CassandraTagSerialized(tagSerialized: TagSerialized, timeUuid: UUID, timeBucket: TimeBucket) {
  def persistenceId: String = tagSerialized.persistenceId
  def sequenceNr: Long = tagSerialized.sequenceNr
}

class CassandraTagWriterPlugin(system: ActorSystem, config: Config)
    extends TagWriterExt {


  private val log = Logging(system, getClass)

  implicit val ec: ExecutionContext = system.dispatcher

  val keyspaceName: String = config.getString("keyspace")

  val tagTable: TableSettings = TableSettings(
    config.getString("events-by-tag.table"),
    CassandraCompactionStrategy(config.getConfig("events-by-tag.compaction-strategy")),
    config.getLong("events-by-tag.gc-grace-seconds"),
    if (config.hasPath("events-by-tag.time-to-live")) Some(config.getDuration("events-by-tag.time-to-live", TimeUnit.MILLISECONDS).millis) else None)


  val sessionSettings: CassandraSessionSettings = CassandraSessionSettings(
    config
  )

  val sessionProvider: SessionProvider =
    SessionProvider(system.asInstanceOf[ExtendedActorSystem], config)

  val tagWriterSettings = TagWriterSettings(
    config.getInt("events-by-tag.max-message-batch-size"),
    config
      .getDuration("events-by-tag.flush-interval", TimeUnit.MILLISECONDS)
      .millis,
    config
      .getDuration(
        "events-by-tag.scanning-flush-interval",
        TimeUnit.MILLISECONDS
      )
      .millis,
    config.getBoolean("pubsub-notification")
  )

  log.info("Running with settings {}", tagWriterSettings)

  val tagWriterPromise = Promise[ActorRef]
  val tagWriters: Future[ActorRef] = tagWriterPromise.future



  override def initialize(): Future[Done] = {

    val s: TaggedPreparedStatements = new TaggedPreparedStatements(ec, keyspaceName, tagTable)

    val session = new CassandraSession(
    system,
    sessionProvider,
    sessionSettings,
    system.dispatcher,
    log,
    metricsCategory = "FIXME",
    init = s.createTables
  )

    log.info("initializing")
    val statements = for {
      _ <- session.executeCreateTable(s.createTagsTable)
      _ <- session.executeCreateTable(s.createTagsProgressTable)
      _ <- session.executeCreateTable(s.createTagScanningTable)
    } yield {
      tagWriterPromise.complete(Try(create(session, s)))
      Done
    }

    // FIXME, chain tag writes on to this future
    Await.ready(statements, 10.seconds)
    log.info("Tables created")


    statements
  }

  private def create(session: CassandraSession,
                     s: TaggedPreparedStatements): ActorRef = {
    import s._

    def preparedWriteToTagViewWithoutMeta: Future[PreparedStatement] =
      session.prepare(writeTags(false)).map(_.setIdempotent(true))
    def preparedWriteToTagViewWithMeta: Future[PreparedStatement] =
      session.prepare(writeTags(true)).map(_.setIdempotent(true))
    def preparedWriteToTagProgress: Future[PreparedStatement] =
      session.prepare(writeTagProgress).map(_.setIdempotent(true))
    def preparedSelectTagProgress: Future[PreparedStatement] =
      session.prepare(selectTagProgress).map(_.setIdempotent(true))
    def preparedSelectTagProgressForPersistenceId: Future[PreparedStatement] =
      session
        .prepare(selectTagProgressForPersistenceId)
        .map(_.setIdempotent(true))
    def preparedWriteTagScanning: Future[PreparedStatement] =
      session.prepare(writeTagScanning).map(_.setIdempotent(true))
    def preparedSelectTagScanningForPersistenceId: Future[PreparedStatement] =
      session
        .prepare(selectTagScanningForPersistenceId)
        .map(_.setIdempotent(true))

    system.actorOf(
      TagWriters.props(
        tagWriterSettings,
        TagWritersSession(
          preparedWriteToTagViewWithoutMeta,
          session.executeWrite,
          session.selectResultSet,
          preparedWriteToTagProgress,
          preparedWriteTagScanning
        )
      )
    )
  }

  override def persistentActorStarting(persistenceId: String,
                                       actorRef: ActorRef): Future[Done] = {
    log.info("persistentActorStarting {} {}", persistenceId, actorRef)
    Future.successful(Done)
  }

  def writeTag(tw: TagWrite): Future[Done] = {
    log.info("writeTag {}", tw)
    val cassandraTagWrite = CassandraTagWrite(tw.persistenceId, tw.write.map(ts => {
      val offset = ts.offset match {
        case TimeBasedUUID(uuid) => uuid
        case _ => UUIDs.timeBased()
      }
      // FIXME from config
      val bucket = TimeBucket(offset, Hour)
      CassandraTagSerialized(ts, offset, bucket)
    }))
    log.info("CassandraTagWrite {}", cassandraTagWrite)
    Future.successful(Done)
  }

  def writeTag(tw: BulkTagWrite): Future[Done] = {
    log.info("write bulk tag {}", tw)
    tagWriters.map(_ ! CassandraBulkTagWrite(tw.writes.map(tw => toCassandraTagWrite(tw)), tw.withoutTags.map(toCassandraSerialized)))
      .map(_ => Done)

  }
}
