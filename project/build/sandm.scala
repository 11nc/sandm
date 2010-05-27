import sbt._
import Process._

class SandMProject(info: ProjectInfo) extends DefaultProject(info) {

  override def packageDocsJar = defaultJarPath("-javadoc.jar")
  override def packageSrcJar= defaultJarPath("-sources.jar")
  val sourceArtifact = Artifact.sources(artifactID)
  val docsArtifact = Artifact.javadoc(artifactID)
  override def packageToPublishActions = super.packageToPublishActions ++ Seq(packageDocs, packageSrc)
  
	override def compileOptions = Deprecation :: ExplainTypes :: Unchecked :: super.compileOptions.toList

  val mongo = "org.mongodb" % "mongo-java-driver" % "1.4"
	
}