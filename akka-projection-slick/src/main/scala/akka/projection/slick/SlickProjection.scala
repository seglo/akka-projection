/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.{ ApiMayChange, InternalApi }
import akka.projection.Projection
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.{ KillSwitch, KillSwitches }
import org.slf4j.LoggerFactory
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.Future
import scala.reflect.ClassTag

object SlickProjection {
  @ApiMayChange
  def transactional[Offset, StreamElement, P <: JdbcProfile: ClassTag](
      projectionId: String,
      sourceProvider: Option[Offset] => Source[StreamElement, _],
      offsetExtractor: StreamElement => Offset,
      databaseConfig: DatabaseConfig[P])(eventHandler: StreamElement => DBIO[Done]): Projection =
    new SlickProjectionImpl(projectionId, sourceProvider, offsetExtractor, databaseConfig, eventHandler)
}

@InternalApi
private[projection] class SlickProjectionImpl[Offset, StreamElement, P <: JdbcProfile](
    projectionId: String,
    sourceProvider: Option[Offset] => Source[StreamElement, _],
    offsetExtractor: StreamElement => Offset,
    databaseConfig: DatabaseConfig[P],
    eventHandler: StreamElement => DBIO[Done])
    extends Projection {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val offsetStore = new OffsetStore(databaseConfig.db, databaseConfig.profile)

  private var shutdown: Option[KillSwitch] = None

  override def start()(implicit systemProvider: ClassicActorSystemProvider): Unit = {

    implicit val dispatcher = systemProvider.classicSystem.dispatcher
    import databaseConfig.profile.api._

    val lastKnownOffset: Future[Option[Offset]] = offsetStore.readOffset(projectionId)

    val killSwitch =
      Source
        .future(lastKnownOffset.map { offsetOpt =>
          logger.debug(s"Starting projection '$projectionId' from offset '$offsetOpt' ")
          sourceProvider(offsetOpt)
        })
        .flatMapConcat[StreamElement, Any](identity)
        .mapAsync(1) { elt =>
          // run user function and offset storage on the same transaction
          // offset storage comes last, any side-effect in user function is therefore at-least-once
          val txDBIO =
            eventHandler(elt).flatMap(_ => offsetStore.saveOffset(projectionId, offsetExtractor(elt))).transactionally

          databaseConfig.db.run(txDBIO)
        }
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.left)
        .run()

    shutdown = Some(killSwitch)
  }

  override def stop(): Future[Done] = {
    shutdown.foreach(_.shutdown())
    Future.successful(Done)
  }
}
