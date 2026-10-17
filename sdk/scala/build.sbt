ThisBuild / organization := "org.dilock"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "3.3.6"
ThisBuild / crossScalaVersions := Seq("3.3.6", "2.13.18")
name := (if (scalaBinaryVersion.value == "2.13") "dilock-scala213" else "dilock-scala3")

Compile / scalaSource := baseDirectory.value / "src" / "main" / (if (scalaBinaryVersion.value == "2.13") "scala-2.13" else "scala")
Test / scalaSource := baseDirectory.value / "src" / "test" / (if (scalaBinaryVersion.value == "2.13") "scala-2.13" else "scala")

// Keep the public Scala 3 quickstarts compiling as part of the normal test build.
Test / unmanagedSources ++= {
  if (scalaBinaryVersion.value == "3")
    Seq(
      baseDirectory.value / "consumers" / "Quickstart.scala",
      baseDirectory.value / "consumers" / "LiveQuickstart.scala"
    )
  else Seq.empty
}

resolvers += {
  val repository = sys.props.get("dilockLocalMavenRepo")
    .map(file)
    .getOrElse(baseDirectory.value / "../../.tools/m2repo")
  "dilock-local-m2" at repository.toURI.toString
}

libraryDependencies ++= Seq(
  "org.dilock" % "dilock-core" % "0.1.0",
  "org.dilock" % "dilock-redis" % "0.1.0" % Provided,
  "org.dilock" % "dilock-etcd" % "0.1.0" % Provided,
  "org.dilock" % "dilock-consul" % "0.1.0" % Provided,
  "org.scalameta" %% "munit" % "1.0.4" % Test,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.21.2" % Test
)

Compile / doc / scalacOptions ++= Seq("-doc-title", "Dilock Scala SDK", "-deprecation", "-feature", "-unchecked")
Test / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
