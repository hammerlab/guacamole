package org.hammerlab.guacamole.util

import org.hammerlab.genomics.bases.BasesUtil
import org.hammerlab.genomics.readsets.PerSample
import org.hammerlab.genomics.reference.test.{ ClearContigNames, LociConversions }
import org.hammerlab.guacamole.kryo.Registrar
import org.hammerlab.spark.test.suite.{ KryoSparkSuite, SparkSerialization }
import org.hammerlab.test.resources.File

abstract class GuacFunSuite
  extends KryoSparkSuite(classOf[Registrar], referenceTracking = true)
    with SparkSerialization
    with BasesUtil
    with LociConversions
    with ClearContigNames {

  sparkConf(
    "spark.app.name" → "guacamole"
  )

  implicit def unwrapFiles(files: PerSample[File]): PerSample[String] = files.map(_.pathStr)
}
