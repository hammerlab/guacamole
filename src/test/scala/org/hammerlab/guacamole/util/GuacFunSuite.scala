package org.hammerlab.guacamole.util

import org.hammerlab.guacamole.kryo.Registrar
import org.hammerlab.spark.test.suite.KryoSparkSuite

class GuacFunSuite
  extends KryoSparkSuite(classOf[Registrar], referenceTracking = true)
    with SparkSerializerSuite {
  conf.setAppName("guacamole")
}

