/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.slick

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import docs.eventsourced.ShoppingCart

//#projection-imports
import akka.projection.ProjectionId
import akka.projection.slick.SlickProjection
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.H2Profile

//#projection-imports

//#handler-imports
import akka.projection.slick.SlickHandler
import scala.concurrent.Future
import akka.Done
import org.slf4j.LoggerFactory

//#handler-imports

class SlickProjectionDocExample {

  //#repository
  case class Order(id: String, time: Instant)

  class OrderRepository(val dbConfig: DatabaseConfig[H2Profile]) {

    import dbConfig.profile.api._

    private class OrdersTable(tag: Tag) extends Table[Order](tag, "ORDERS") {
      def id = column[String]("CART_ID", O.PrimaryKey)

      def time = column[Instant]("TIME")

      def * = (id, time).mapTo[Order]
    }

    private val ordersTable = TableQuery[OrdersTable]

    def save(order: Order)(implicit ec: ExecutionContext) = {
      ordersTable.insertOrUpdate(order).map(_ => Done)
    }

    def createTable(): Future[Unit] =
      dbConfig.db.run(ordersTable.schema.createIfNotExists)
  }
  //#repository

  //#handler
  class ShoppingCartHandler(repository: OrderRepository)(implicit ec: ExecutionContext)
      extends SlickHandler[EventEnvelope[ShoppingCart.Event]] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[ShoppingCart.Event]): DBIO[Done] = {
      envelope.event match {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info("Shopping cart {} was checked out at {}", cartId, time)
          repository.save(Order(cartId, time))

        case otherEvent =>
          logger.debug("Shopping cart {} changed by {}", otherEvent.cartId, otherEvent)
          DBIO.successful(Done)
      }
    }
  }
  //#handler

  private val system = ActorSystem[Nothing](Behaviors.empty, "Example")
  //#db-config
  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", system.settings.config)

  val repository = new OrderRepository(dbConfig)
  //#db-config

  //#sourceProvider
  val sourceProvider =
    EventSourcedProvider
      .eventsByTag[ShoppingCart.Event](system, readJournalPluginId = JdbcReadJournal.Identifier, tag = "carts-1")
  //#sourceProvider

  object IllustrateExactlyOnce {
    //#exactlyOnce
    implicit val ec = system.executionContext

    val projection =
      SlickProjection.exactlyOnce(
        projectionId = ProjectionId("ShoppingCarts", "carts-1"),
        sourceProvider,
        dbConfig,
        handler = new ShoppingCartHandler(repository))
    //#exactlyOnce
  }

  object IllustrateAtLeastOnce {
    //#atLeastOnce
    implicit val ec = system.executionContext

    val projection =
      SlickProjection
        .atLeastOnce(
          projectionId = ProjectionId("ShoppingCarts", "carts-1"),
          sourceProvider,
          dbConfig,
          handler = new ShoppingCartHandler(repository))
        .withSaveOffsetAfterEnvelopes(100)
        .withSaveOffsetAfterDuration(500.millis)
    //#atLeastOnce
  }

}
