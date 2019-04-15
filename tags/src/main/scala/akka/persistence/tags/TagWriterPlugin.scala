package akka.persistence.tags

import java.nio.ByteBuffer

import akka.Done
import akka.actor.{ ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId }
import akka.persistence.query.Offset
import scala.collection.immutable

import scala.concurrent.Future

case class TagSerialized(persistenceId: String, sequenceNr: Long, serialized: ByteBuffer, tags: Set[String],
  eventAdapterManifest: String, serManifest: String, serId: Int, writerUuid: String,
  offset: Offset)

/**
 * Single tag write that can be made up of multiple [[TagSerialized]] if persistAll is used
 */
case class TagWrite(tag: String, persistenceId: String, write: List[TagSerialized])

/**
 * Events without tags included in case an house keeping needs to be done to make replay more effecient.
 */
case class BulkTagWrite(writes: immutable.Seq[TagWrite], withoutTags: immutable.Seq[TagSerialized])

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
  def writeTag(tw: TagWrite): Future[Done]

  def writeTag(tw: BulkTagWrite): Future[Done]
}
