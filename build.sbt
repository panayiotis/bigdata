name := "bigdata"

assemblyJarName in assembly := s"${name.value}.jar"

version := "0.0.1"

scalaVersion := "2.10.5"

val sparkVersion = "1.6.0"

val sparkDependencyScope = "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % sparkVersion % sparkDependencyScope,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % sparkDependencyScope,
  "org.scalatest"    %% "scalatest"   % "2.2.6"      % "test"/*,
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()*/
)
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))

test in assembly := {}

parallelExecution in Test := false
