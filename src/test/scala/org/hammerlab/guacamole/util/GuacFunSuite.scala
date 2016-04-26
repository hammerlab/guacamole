package org.hammerlab.guacamole.util

import org.bdgenomics.utils.misc.SparkFunSuite
import org.scalatest.Matchers

trait GuacFunSuite extends SparkFunSuite with Matchers {
  override val appName: String = "guacamole"

  def registrar = "org.hammerlab.guacamole.util.KryoTestRegistrar"

  override val properties: Map[String, String] =
    Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryo.registrator" -> registrar,
      "spark.kryoserializer.buffer" -> "4",
      "spark.kryo.registrationRequired" -> "true",
      "spark.kryo.referenceTracking" -> "true",
      "spark.driver.host" -> "localhost"
    )
}

