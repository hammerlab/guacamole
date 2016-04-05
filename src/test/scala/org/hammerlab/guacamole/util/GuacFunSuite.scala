package org.hammerlab.guacamole.util

import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.Matchers

trait GuacFunSuite extends SparkFunSuite with Matchers {
  override val appName: String = "guacamole"
  override val properties: Map[String, String] =
    Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryo.registrator" -> "org.hammerlab.guacamole.GuacamoleKryoRegistrator",
      "spark.kryoserializer.buffer" -> "4",
      "spark.kryo.referenceTracking" -> "true"
    )

}

