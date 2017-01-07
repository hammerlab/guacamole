name := "guacamole"
version := "0.1.0-SNAPSHOT"

sparkVersion := "1.6.1"
hadoopVersion := "2.7.2"

addSparkDeps

deps ++= Seq(
  "org.hammerlab.adam" %% "adam-core" % "0.20.3",
  libs.value('args4j),
  "org.hammerlab" %% "args4s" % "1.0.0",
  libs.value('bdg_formats),
  libs.value('bdg_utils_cli),
  libs.value('breeze),
  libs.value('commons_math),
  libs.value('hadoop_bam),
  libs.value('htsjdk),
  "org.hammerlab" %% "genomic-loci" % "1.4.4",
  libs.value('magic_rdds),
  libs.value('slf4j),
  libs.value('spark_commands),
  libs.value('spark_util),
  libs.value('spire),
  libs.value('string_utils)
)

providedDeps += libs.value('mllib)

takeFirstLog4JProperties

mainClass := Some("org.hammerlab.guacamole.Main")

shadedDeps += "org.scalanlp" %% "breeze" % "0.12"
shadeRenames += "breeze.**" -> "org.hammerlab.breeze.@1"

//publishThinShadedJar
