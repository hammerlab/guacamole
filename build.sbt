name := "guacamole"
version := "0.1.0-SNAPSHOT"

sparkVersion := "1.6.1"
hadoopVersion := "2.7.2"

providedDeps ++= Seq(
  libraries.value('spark),
  libraries.value('hadoop),
  libraries.value('mllib)
)

libraryDependencies ++= Seq(
  libraries.value('args4j),
  libraries.value('args4s),
  libraries.value('bdg_formats),
  libraries.value('hadoop_bam),
  libraries.value('kryo),
  libraries.value('spark_commands),
  libraries.value('spark_util),
  libraries.value('spire),
  "org.hammerlab.adam" %% "adam-core" % "0.20.3",
  "org.bdgenomics.utils" %% "utils-cli" % "0.2.10",
  "org.hammerlab" %% "magic-rdds" % "1.3.1",
  "org.hammerlab" %% "genomic-loci" % "1.4.3",
  "com.github.samtools" % "htsjdk" % "2.6.1" exclude("org.xerial.snappy", "snappy-java"),
  "org.apache.commons" % "commons-math3" % "3.0",
  "org.clapper" %% "grizzled-slf4j" % "1.0.3"
)

testDeps ++= Seq(
  libraries.value('spark_testing_base),
  libraries.value('hadoop),
  libraries.value('spark_tests)
)

assemblyMergeStrategy in assembly := {
  // Two org.bdgenomics deps include the same log4j.properties.
  case PathList("log4j.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

mainClass := Some("org.hammerlab.guacamole.Main")

shadedDeps += "org.scalanlp" %% "breeze" % "0.12"
shadeRenames += "breeze.**" -> "org.hammerlab.breeze.@1"

//ParentPlugin.publishThinShadedJar
