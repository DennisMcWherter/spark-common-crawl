import sbt._


object Plugins extends Build {
  lazy val root = Project("root", file(".")) dependsOn {
    uri("git://github.com/sbt/sbt-assembly.git#0.12.0")
  }
}

// vim: set ts=4 sw=4 et:
