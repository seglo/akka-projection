/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import akka.actor.ActorSystem
import akka.projection.testkit.ProjectionTestRunner
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{ BeforeAndAfterAll, Suite }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import slick.basic.DatabaseConfig
import slick.jdbc.H2Profile

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.testkit.typed.scaladsl.ActorTestKit

abstract class SlickSpec(config: Config) extends Suite with BeforeAndAfterAll {

  private val testKit = ActorTestKit("slick-test", config.withFallback(ConfigFactory.load()))

  implicit def actorSystem: ActorSystem = testKit.internalSystem.classicSystem

  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", config)

  val offsetStore = new OffsetStore(dbConfig.db, dbConfig.profile)

  override protected def beforeAll(): Unit = {
    // create offset table
    Await.ready(offsetStore.createIfNotExists, 3.seconds)
  }

  override protected def afterAll(): Unit = {
    testKit.shutdownTestKit()
    dbConfig.db.close()
  }
}
