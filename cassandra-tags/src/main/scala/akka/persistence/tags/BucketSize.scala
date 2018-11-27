package akka.persistence.tags

import scala.concurrent.duration._

private[akka] sealed trait BucketSize {
  val durationMillis: Long
}

private[akka] case object Day extends BucketSize {
  override val durationMillis: Long = 1.day.toMillis
}
private[akka] case object Hour extends BucketSize {
  override val durationMillis: Long = 1.hour.toMillis
}
private[akka] case object Minute extends BucketSize {
  override val durationMillis: Long = 1.minute.toMillis
}

// Not to be used for real production apps. Just to make testing bucket transitions easier.
private[akka] case object Second extends BucketSize {
  override val durationMillis: Long = 1.second.toMillis
}

private[akka] object BucketSize {
  def fromString(value: String): BucketSize = {
    Vector(Day, Hour, Minute, Second).find(_.toString.toLowerCase == value.toLowerCase)
      .getOrElse(throw new IllegalArgumentException("Invalid value for bucket size: " + value))
  }
}

