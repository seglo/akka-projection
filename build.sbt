import akka.projections.Dependencies

scalaVersion := "2.13.1"

val commonSettings = Seq(
  organization := "com.lightbend.akka",
  scalacOptions ++= List(
      "-unchecked",
      "-deprecation",
      "-language:_",
      "-Xfatal-warnings",
      "-Ywarn-unused",
      "-encoding",
      "UTF-8"),
  javacOptions ++= List("-Xlint:unchecked", "-Xlint:deprecation"))

lazy val akkaProjectionCore =
  Project(id = "akka-projection-core", base = file("akka-projection-core")).settings(Dependencies.core)

lazy val akkaProjectionTestkit = Project(id = "akka-projection-testkit", base = file("akka-projection-testkit"))
  .settings(libraryDependencies ++= Seq(Dependencies.Test.scalaTest))
  .dependsOn(akkaProjectionCore)

lazy val root = Project(id = "akka-projection", base = file(".")).aggregate(akkaProjectionCore, akkaProjectionTestkit)

// check format and headers
TaskKey[Unit]("verifyCodeFmt") := {
  scalafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Scala code found. Please run 'scalafmtAll' and commit the reformatted code")
  }
  (Compile / scalafmtSbtCheck).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted sbt code found. Please run 'scalafmtSbt' and commit the reformatted code")
  }
}

addCommandAlias("verifyCodeStyle", "headerCheckAll; verifyCodeFmt")
