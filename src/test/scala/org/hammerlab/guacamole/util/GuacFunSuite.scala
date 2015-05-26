package org.hammerlab.guacamole.util

import org.bdgenomics.utils.misc.SparkFunSuite

trait GuacFunSuite extends SparkFunSuite {
  override val appName: String = "guacamole"
  override val properties: Map[String, String] =
    Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryo.registrator" -> "org.hammerlab.guacamole.GuacamoleKryoRegistrator",
      "spark.kryoserializer.buffer.mb" -> "4",
      "spark.kryo.referenceTracking" -> "true"
    )

}

