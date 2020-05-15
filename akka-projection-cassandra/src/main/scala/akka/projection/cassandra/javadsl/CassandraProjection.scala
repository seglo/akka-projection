/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.projection.AtLeastOnceSettings
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.cassandra.internal.HandlerAdapter
import akka.projection.cassandra.scaladsl
import akka.projection.internal.SourceProviderAdapter
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider

/**
 * Factories of [[Projection]] where the offset is stored in Cassandra. The envelope handler can
 * integrate with anything, such as publishing to a message broker, or updating a read model in Cassandra.
 */
@ApiMayChange
object CassandraProjection {

  /**
   * Create a [[Projection]] with at-least-once processing semantics. It stores the offset in Cassandra
   * after the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset some elements may be processed more than once.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[Envelope]): AtLeastOnceCassandraProjection[Envelope] =
    scaladsl.CassandraProjection
      .atLeastOnce(projectionId, new SourceProviderAdapter(sourceProvider), new HandlerAdapter(handler))
      .asInstanceOf[AtLeastOnceCassandraProjection[
        Envelope
      ]] // TODO: refactor SourceProviderAdapter into CassandraProjectionProviderAdapter

  /**
   * Create a [[Projection]] with at-most-once processing semantics. It stores the offset in Cassandra
   * before the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset one envelope may not have been processed.
   */
  def atMostOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[Envelope]): AtMostOnceCassandraProjection[Envelope] =
    scaladsl.CassandraProjection
      .atMostOnce(projectionId, new SourceProviderAdapter(sourceProvider), new HandlerAdapter(handler))
      .asInstanceOf[AtMostOnceCassandraProjection[
        Envelope
      ]] // TODO: refactor SourceProviderAdapter into CassandraProjectionProviderAdapter
}

@DoNotInherit trait CassandraProjection[Envelope] extends Projection[Envelope] {

  override def withSettings(settings: ProjectionSettings): CassandraProjection[Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def initializeOffsetTable(systemProvider: ClassicActorSystemProvider): CompletionStage[Done]

}

@DoNotInherit trait AtLeastOnceCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  override def withSettings(settings: ProjectionSettings): AtLeastOnceCassandraProjection[Envelope]

  def withAtLeastOnceSettings(settings: AtLeastOnceSettings): AtLeastOnceCassandraProjection[Envelope]
  def withSaveOffsetAfterEnvelopes(afterEnvelopes: Int): AtLeastOnceCassandraProjection[Envelope]
  def withSaveOffsetAfterDuration(afterDuration: java.time.Duration): AtLeastOnceCassandraProjection[Envelope]
}

@DoNotInherit trait AtMostOnceCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  override def withSettings(settings: ProjectionSettings): AtMostOnceCassandraProjection[Envelope]
}
