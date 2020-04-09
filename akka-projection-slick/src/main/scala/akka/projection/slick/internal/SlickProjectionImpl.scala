/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.event.Logging
import akka.projection.Projection
import akka.projection.slick.OffsetStore
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.stream.{ KillSwitch, KillSwitches }
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.Future
@InternalApi
private[projection] class SlickProjectionImpl[Offset, StreamElement, P <: JdbcProfile](
    projectionId: String,
    sourceProvider: Option[Offset] => Source[StreamElement, _],
    offsetExtractor: StreamElement => Offset,
    databaseConfig: DatabaseConfig[P],
    eventHandler: StreamElement => DBIO[Done])
    extends Projection {

  private val offsetStore = new OffsetStore(databaseConfig.db, databaseConfig.profile)

  private var shutdown: Option[KillSwitch] = None

  override def start()(implicit systemProvider: ClassicActorSystemProvider): Unit = {

    // TODO: add a LogSource for projection when we have a name and key
    val akkaLogger = Logging(systemProvider.classicSystem, this.getClass)

    implicit val dispatcher = systemProvider.classicSystem.dispatcher
    import databaseConfig.profile.api._

    val lastKnownOffset: Future[Option[Offset]] = offsetStore.readOffset(projectionId)

    val killSwitch =
      Source
        .future(lastKnownOffset.map { offsetOpt =>
          akkaLogger.debug("Starting projection '{}' from offset '{}'", projectionId, offsetOpt)
          sourceProvider(offsetOpt)
        })
        .flatMapConcat[StreamElement, Any](identity)
        .viaMat(KillSwitches.single)(Keep.right)
        .mapAsync(1) { elt =>
          // run user function and offset storage on the same transaction
          // any side-effect in user function is at-least-once
          val txDBIO =
            offsetStore.saveOffset(projectionId, offsetExtractor(elt)).flatMap(_ => eventHandler(elt))

          databaseConfig.db.run(txDBIO.transactionally)
        }
        .toMat(Sink.ignore)(Keep.left)
        .run()

    shutdown = Some(killSwitch)
  }

  override def stop(): Future[Done] = {
    shutdown.foreach(_.shutdown())
    Future.successful(Done)
  }
}
