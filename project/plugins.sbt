// You may use this file to add plugin dependencies for sbt.

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

// scapegoat: static analysis compiler plugin
//addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "0.94.6")

// scalastyle: coding style check and enforcer
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

// sbt-pack
addSbtPlugin("org.xerial.sbt" %% "sbt-pack" % "0.6.12")  // for sbt-0.13.x or higher