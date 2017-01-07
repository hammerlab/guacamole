name := "guacamole"
version := "0.1.0-SNAPSHOT"

sparkVersion := "1.6.1"
hadoopVersion := "2.7.2"

addSparkDeps

deps ++= Seq(
  libs.value('args4j),
  "org.hammerlab" %% "args4s" % "1.0.0",
  libs.value('bdg_formats),
  libs.value('hadoop_bam),
  "org.hammerlab" %% "genomic-loci" % "1.4.3",
  libs.value('spark_commands),
  libs.value('spark_util),
  libs.value('spire),
  "org.hammerlab.adam" %% "adam-core" % "0.20.3",
  "org.bdgenomics.utils" %% "utils-cli" % "0.2.10",
  "org.hammerlab" %% "magic-rdds" % "1.3.1",
  "com.github.samtools" % "htsjdk" % "2.6.1" exclude("org.xerial.snappy", "snappy-java"),
  "org.apache.commons" % "commons-math3" % "3.0",
  "org.clapper" %% "grizzled-slf4j" % "1.0.3"
)

providedDeps += libs.value('mllib)

takeFirstLog4JProperties

mainClass := Some("org.hammerlab.guacamole.Main")

shadedDeps += "org.scalanlp" %% "breeze" % "0.12"
shadeRenames += "breeze.**" -> "org.hammerlab.breeze.@1"

//publishThinShadedJar
