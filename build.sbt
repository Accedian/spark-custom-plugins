val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "spark-custom-plugins",
    organization := "com.accedian",
    scalaVersion := "2.12.20",
    version := "3.5.3-0.0.1",
    libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
    )
  )


publishMavenStyle := true


publishTo := {
  if (isSnapshot.value)
    Some(Resolver.file("file",  new File( "maven-repo/snapshots" )) )
  else
    Some(Resolver.file("file",  new File( "maven-repo/releases" )) )
}