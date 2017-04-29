import sbtassembly.PathList

name := "guacamole"
version := "0.1.0-SNAPSHOT"

addSparkDeps

deps ++= Seq(
  libs.value('adam_core),
  libs.value('args4j),
  libs.value('args4s),
  libs.value('bdg_formats),
  libs.value('bdg_utils_cli),
  libs.value('breeze),
  libs.value('commons_math),
  libs.value('hadoop_bam).copy(revision = "7.8.1-SNAPSHOT"),
  "com.google.cloud" % "google-cloud-nio" % "0.10.0-alpha",
  libs.value('htsjdk),
  libs.value('iterators),
  libs.value('magic_rdds),
  libs.value('paths),
  libs.value('scalautils),
  libs.value('slf4j),
  libs.value('spark_commands),
  libs.value('spark_util),
  libs.value('spire),
  libs.value('string_utils)
)

compileAndTestDeps ++= Seq(
  libs.value('genomic_utils),
  libs.value('loci),
  libs.value('reads),
  libs.value('readsets),
  libs.value('reference)
)

providedDeps += libs.value('mllib)

takeFirstLog4JProperties

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

main := "org.hammerlab.guacamole.Main"

shadedDeps ++= Seq(
  "org.scalanlp" %% "breeze" % "0.12"
)

shadeRenames ++= Seq(
  "breeze.**" -> "org.hammerlab.breeze.@1",

  // google-cloud-nio uses Guava 19.0 and at least one API (Splitter.splitToList) that succeeds Spark's Guava (14.0.1).
  "com.google.common.**" -> "org.hammerlab.guava.common.@1",

  // GCS Connector uses an older google-api-services-storage than google-cloud-nio and breaks us without shading it in
  // here; see http://stackoverflow.com/a/39521403/544236.
  "com.google.api.services.**" -> "hammerlab.google.api.services.@1"
)

//publishThinShadedJar
