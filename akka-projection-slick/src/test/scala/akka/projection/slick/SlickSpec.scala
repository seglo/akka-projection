/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import akka.actor.ActorSystem
import akka.projection.testkit.ProjectionTestRunner
import com.typesafe.config.Config
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import slick.basic.DatabaseConfig
import slick.jdbc.H2Profile

import scala.concurrent.Await
import scala.concurrent.duration._

abstract class SlickSpec(config: Config)
    extends AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with ProjectionTestRunner
    with BeforeAndAfterAll
    with Eventually {

  private var _actorSystem: ActorSystem = _

  implicit def actorSystem: ActorSystem = _actorSystem

  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", config)

  val offsetStore = new OffsetStore(dbConfig.db, dbConfig.profile)

  override protected def beforeAll(): Unit = {
    _actorSystem = ActorSystem("slick-test")
    // create offset table
    Await.ready(offsetStore.createIfNotExists, 3.seconds)
  }

  override protected def afterAll(): Unit = {
    _actorSystem.terminate().futureValue
    dbConfig.db.close()
  }
}
