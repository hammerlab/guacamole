name := "guacamole"
version := "0.1.0-SNAPSHOT"

hadoopVersion := "2.7.2"

addSparkDeps

deps ++= Seq(
  libs.value('adam_core),
  libs.value('args4j),
  libs.value('args4s),
  libs.value('bdg_formats),
  libs.value('bdg_utils_cli),
  libs.value('breeze),
  libs.value('commons_math),
  libs.value('hadoop_bam),
  libs.value('htsjdk),
  libs.value('iterators),
  libs.value('magic_rdds),
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

main := "org.hammerlab.guacamole.Main"

shadedDeps += "org.scalanlp" %% "breeze" % "0.12"
shadeRenames += "breeze.**" -> "org.hammerlab.breeze.@1"

//publishThinShadedJar
