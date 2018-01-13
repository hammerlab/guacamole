name := "guacamole"
version := "0.1.0-SNAPSHOT"

addSparkDeps

deps ++= Seq(
  adam % "0.23.2",
  args4j,
  args4s % "1.3.0",
  bdg_formats,
  bdg_utils_cli % "0.3.0",
  commons_math,
  seqdoop_hadoop_bam % "7.9.0",
  htsjdk,
  iterators % "2.0.0",
  magic_rdds % "4.1.0",
  paths % "1.3.1",
  scalautils,
  slf4j,
  hammerlab("cli", "args4j") % "1.2.0",
  spark_util % "2.0.1",
  spire,
  string_utils % "1.2.0"
)

compileAndTestDeps ++= Seq(
  genomic_utils % "1.3.1",
  loci % "2.0.1",
  reads % "1.0.6",
  readsets % "1.2.0",
  reference % "1.4.0"
)

providedDeps += mllib

takeFirstLog4JProperties

main := "org.hammerlab.guacamole.Main"

shadedDeps += breeze

shadeRenames ++= Seq(
  "breeze.**" -> "org.hammerlab.breeze.@1",

  // google-cloud-nio uses Guava 19.0 and at least one API (Splitter.splitToList) that succeeds Spark's Guava (14.0.1).
  "com.google.common.**" -> "org.hammerlab.guava.common.@1"
)

//publishThinShadedJar
