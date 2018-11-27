package akka.persistence.tags
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, ExtendedActorSystem}
import akka.cassandra.session.{CassandraSessionSettings, SessionProvider}
import akka.cassandra.session.scaladsl.CassandraSession
import akka.event.Logging
import akka.persistence.tags.TagWriters.BulkTagWrite
import com.datastax.driver.core.{PreparedStatement, Session}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

case class CassandraTagSerialized(tagWrite: TagWriteCat, timeUuid: UUID, timeBucket: TimeBucket)

class CassandraTagWriterPlugin(system: ActorSystem, config: Config) extends TagWriterExt with TaggedPreparedStatements {

  private val log = Logging(system, getClass)

  implicit val ec: ExecutionContext = system.dispatcher

  val sessionSettings: CassandraSessionSettings = CassandraSessionSettings(config)

  val sessionProvider: SessionProvider = SessionProvider(system.asInstanceOf[ExtendedActorSystem], config)

 val tagWriterSettings = TagWriterSettings(
    config.getInt("events-by-tag.max-message-batch-size"),
    config.getDuration("events-by-tag.flush-interval", TimeUnit.MILLISECONDS).millis,
    config.getDuration("events-by-tag.scanning-flush-interval", TimeUnit.MILLISECONDS).millis,
    config.getBoolean("pubsub-notification"))

  // FIXME, make an extension for CassandraSession that loads a single from configuration
  val session = new CassandraSession(
    system,
    sessionProvider,
    sessionSettings,
    system.dispatcher,
    log,
    metricsCategory = "FIXME",
    init = (session: Session) => Future.successful(Done))

  val tagWriters = system.actorOf(TagWriters.props(tagWriterSettings, TagWritersSession(
        preparedWriteToTagViewWithoutMeta,
        session.executeWrite,
        session.selectResultSet,
        preparedWriteToTagProgress,
        preparedWriteTagScanning
  )))

  override def initialize(): Future[Done] = {
    // FIXME intitialize prepared statements
    Future.successful(Done)
  }

  override def persistentActorStarting(persistenceId: String, actorRef: ActorRef): Future[Done] = {
    Future.successful(Done)
  }

  def writeTag(tw: TagWriteCat): Future[Done] = ???

  def writeTag(tw: BulkTagWriteCat): Future[Done] = ???

}
