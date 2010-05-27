import sbt._
import Process._

class SandMProject(info: ProjectInfo) extends DefaultProject(info) {
  
	override def compileOptions = Deprecation :: ExplainTypes :: Unchecked :: super.compileOptions.toList

  val mongo = "org.mongodb" % "mongo-java-driver" % "1.4"
	
}