package akka.persistence.tags

import java.nio.ByteBuffer

import akka.Done
import akka.actor.{ ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId }
import akka.persistence.query.Offset

import scala.concurrent.Future

case class EventSerialized(persistenceId: String, sequenceNr: Long, serialized: ByteBuffer, tags: Set[String],
  eventAdapterManifest: String, serManifest: String, serId: Int, writerUuid: String,
  offset: Offset)

case class TagWriteCat(tag: String, persistenceId: String, sequenceNr: Long, write: EventSerialized)
// FIXME immutable
case class BulkTagWriteCat(writes: List[TagWriteCat])

object TagWriterPlugin extends ExtensionId[TagWriterPlugin] {
  override def get(system: ActorSystem): TagWriterPlugin = super.get(system)

  override def createExtension(system: ExtendedActorSystem): TagWriterPlugin = new TagWriterPlugin()
}

class TagWriterPlugin extends Extension {

}

// TODO notified of a persistent actor starting
// tag writer should be loading its state and responding

trait TagWriterExt {
  def initialize(): Future[Done]

  def persistentActorStarting(persistenceId: String, actorRef: ActorRef): Future[Done]

  // Streams based?
  def writeTag(tw: TagWriteCat): Future[Done]

  def writeTag(tw: BulkTagWriteCat): Future[Done]
}
