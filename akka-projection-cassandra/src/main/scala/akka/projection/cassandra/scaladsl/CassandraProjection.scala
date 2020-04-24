/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.annotation.ApiMayChange
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.cassandra.internal.CassandraProjectionImpl
import akka.projection.scaladsl.SourceProvider

@ApiMayChange
object CassandraProjection {

  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      saveOffsetAfterEnvelopes: Int,
      saveOffsetAfterDuration: FiniteDuration)(handler: Envelope => Future[Done]): Projection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      CassandraProjectionImpl.AtLeastOnce(saveOffsetAfterEnvelopes, saveOffsetAfterDuration),
      handler)

  def atMostOnce[Offset, Envelope](projectionId: ProjectionId, sourceProvider: SourceProvider[Offset, Envelope])(
      handler: Envelope => Future[Done]): Projection[Envelope] =
    new CassandraProjectionImpl(projectionId, sourceProvider, CassandraProjectionImpl.AtMostOnce, handler)
}
