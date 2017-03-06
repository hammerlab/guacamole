package org.hammerlab.guacamole.util

import org.hammerlab.guacamole.kryo.Registrar
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.spark.test.suite.{ KryoSparkSuite, SparkSerialization }
import org.hammerlab.test.resources.File

class GuacFunSuite
  extends KryoSparkSuite(classOf[Registrar], referenceTracking = true)
    with SparkSerialization {
  conf.setAppName("guacamole")

  implicit def unwrapFiles(files: PerSample[File]): PerSample[String] = files.map(_.path)
}

