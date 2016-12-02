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
  "org.hammerlab.adam" %% "adam-core" % "0.20.3",
  libraries.value('hadoop_bam),
  "org.bdgenomics.quinine" %% "quinine-core" % "0.0.2" exclude("org.bdgenomics.adam", "adam-core"),
  "org.hammerlab" %% "magic-rdds" % "1.3.1",
  "org.hammerlab" %% "spark-util" % "1.1.1",
  "org.hammerlab" %% "genomic-loci" % "1.4.2",
  "com.esotericsoftware.kryo" % "kryo" % "2.21",
  "args4j" % "args4j" % "2.33",
  "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.10.0",
  libraries.value('spire),
  "com.github.samtools" % "htsjdk" % "2.6.1" exclude("org.xerial.snappy", "snappy-java"),
  "org.apache.commons" % "commons-math3" % "3.0",
  "org.hammerlab" %% "args4s" % "1.0.0",
  "org.clapper" %% "grizzled-slf4j" % "1.0.3"
)

testDeps ++= Seq(
  libraries.value('spark_testing_base),
  libraries.value('hadoop),
  "org.hammerlab" %% "spark-tests" % "1.1.2"
)

assemblyMergeStrategy in assembly := {
  // Two org.bdgenomics deps include the same log4j.properties.
  case PathList("log4j.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

shadedDeps += "org.scalanlp" %% "breeze" % "0.12"
shadeRenames += "breeze.**" -> "org.hammerlab.breeze.@1"

//ParentPlugin.publishThinShadedJar
