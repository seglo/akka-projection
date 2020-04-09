/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import java.util.UUID

import akka.stream.scaladsl.Source
import akka.{ Done, NotUsed }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.OptionValues
import org.slf4j.LoggerFactory
import slick.basic.DatabaseConfig
import slick.dbio.DBIOAction
import slick.jdbc.H2Profile

import scala.concurrent.Future
import scala.concurrent.duration._

object SlickProjectionSpec {
  def config: Config = ConfigFactory.parseString("""
      |
      |akka {
      | loglevel = "DEBUG"
      | projection.slick = {
      |
      |   profile = "slick.jdbc.H2Profile$"
      |
      |   # TODO: configure connection pool and slick async executor
      |   db = {
      |    url = "jdbc:h2:mem:test1"
      |    driver = org.h2.Driver
      |    connectionPool = disabled
      |    keepAliveConnection = true
      |   }
      | }
      |}
      |""".stripMargin)
}

class SlickProjectionSpec extends SlickSpec(SlickProjectionSpec.config) with OptionValues {

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val patience: PatienceConfig = PatienceConfig(10.seconds, 500.millis)

  val repository = new TestRepository(dbConfig)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    repository.createTable()
  }

  "A Slick projection" must {

    "persist events and offset in same transactional" in {

      implicit val dispatcher = actorSystem.dispatcher

      val projectionId = UUID.randomUUID().toString
      val entityId = UUID.randomUUID().toString

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val slickProjection =
        SlickProjection.transactional(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope => repository.concatToText(envelope.id, envelope.message) }

      runProjection(slickProjection) {
        eventually {
          withClue("check - all values were concatenated") {
            val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
            concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
          }
        }
      }
      withClue("check - all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }

    "restart from previous offset " in {

      implicit val dispatcher = actorSystem.dispatcher

      val projectionId = UUID.randomUUID().toString
      val entityId = UUID.randomUUID().toString

      withClue("check - offset is empty") {
        val offsetOpt = offsetStore.readOffset[Long](projectionId).futureValue
        offsetOpt shouldBe empty
      }

      val slickProjectionFailing =
        SlickProjection.transactional(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope =>
          if (envelope.offset == 4L) DBIOAction.failed(new RuntimeException("fail on third element"))
          else repository.concatToText(envelope.id, envelope.message)
        }

      runProjection(slickProjectionFailing) {
        eventually {
          withClue("check: projection is interrupted on fourth element") {
            val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
            concatStr.text shouldBe "abc|def|ghi"
          }
        }
      }
      withClue("check: last seen offset is 3L") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 3L
      }

      // re-run projection without failing function
      val slickProjection =
        SlickProjection.transactional(
          projectionId = projectionId,
          sourceProvider = sourceProvider(entityId),
          offsetExtractor = offsetExtractor,
          databaseConfig = dbConfig) { envelope => repository.concatToText(envelope.id, envelope.message) }

      runProjection(slickProjection) {
        eventually {
          withClue("checking: all values were concatenated") {
            val concatStr = dbConfig.db.run(repository.findById(entityId)).futureValue.value
            concatStr.text shouldBe "abc|def|ghi|jkl|mno|pqr"
          }
        }
      }

      withClue("check: all offsets were seen") {
        val offset = offsetStore.readOffset[Long](projectionId).futureValue.value
        offset shouldBe 6L
      }
    }
  }

  case class Envelope(id: String, offset: Long, message: String)

  def offsetExtractor(env: Envelope): Long = env.offset

  def sourceProvider(id: String)(offset: Option[Long]): Source[Envelope, NotUsed] = {
    val elements =
      List(
        Envelope(id, 1L, "abc"),
        Envelope(id, 2L, "def"),
        Envelope(id, 3L, "ghi"),
        Envelope(id, 4L, "jkl"),
        Envelope(id, 5L, "mno"),
        Envelope(id, 6L, "pqr"))

    val src = Source.fromIterator(() => elements.toIterator)

    offset match {
      case Some(o) => src.dropWhile(_.offset <= o)
      case _       => src
    }
  }

  // test model is as simple as a text that gets other string concatenated to it
  case class ConcatStr(id: String, text: String) {
    def concat(newMsg: String) = copy(text = text + "|" + newMsg)
  }

  class TestRepository(val dbConfig: DatabaseConfig[H2Profile]) {

    import dbConfig.profile.api._

    private class ConcatStrTable(tag: Tag) extends Table[ConcatStr](tag, "TEST_MODEL") {
      def id = column[String]("ID", O.PrimaryKey)

      def concatenated = column[String]("CONCATENATED")

      def * = (id, concatenated).mapTo[ConcatStr]
    }

    def concatToText(id: String, payload: String) = {
      // map using Slick's own EC
      implicit val ec = dbConfig.db.executor.executionContext
      for {
        concatStr <- findById(id).map {
          case Some(concatStr) => concatStr.concat(payload)
          case _               => ConcatStr(id, payload)
        }
        _ <- concatStrTable.insertOrUpdate(concatStr)
      } yield Done
    }

    def findById(id: String): DBIO[Option[ConcatStr]] =
      concatStrTable.filter(_.id === id).result.headOption

    private val concatStrTable = TableQuery[ConcatStrTable]

    def readValue(id: String): Future[String] = {
      // map using Slick's own EC
      implicit val ec = dbConfig.db.executor.executionContext
      val action = findById(id).map {
        case Some(concatStr) => concatStr.text
        case _               => "N/A"
      }
      dbConfig.db.run(action)
    }

    def createTable(): Future[Unit] =
      dbConfig.db.run(concatStrTable.schema.createIfNotExists)
  }

}
