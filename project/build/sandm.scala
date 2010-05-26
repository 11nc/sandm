import sbt._
import Process._

class SandMProject(info: ProjectInfo) extends DefaultProject(info) {
  
	override def compileOptions = Deprecation :: ExplainTypes :: Unchecked :: super.compileOptions.toList

  // val specs = "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test->default"

  // val configgyrepo = "Configgy's Repository" at "http://www.lag.net/repo"
  // val configgy = "net.lag" % "configgy" % "1.5"
  val mongo = "org.mongodb" % "mongo-java-driver" % "1.4"
	
}