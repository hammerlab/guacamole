package org.hammerlab.guacamole.util

import org.hammerlab.guacamole.kryo.Registrar
import org.hammerlab.spark.test.suite.{ KryoSparkSuite, SparkSerialization }

class GuacFunSuite
  extends KryoSparkSuite(classOf[Registrar], referenceTracking = true)
    with SparkSerialization {
  conf.setAppName("guacamole")
}

