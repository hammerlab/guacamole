package org.hammerlab.guacamole.util

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FunSuite, Matchers}

trait GuacFunSuite extends FunSuite with SharedSparkContext with Matchers with SparkSerializerSuite {
  conf.setAppName("guacamole")

  def registrar = "org.hammerlab.guacamole.kryo.GuacamoleKryoRegistrar"

  val properties: Map[String, String] =
    Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryo.registrator" -> registrar,
      "spark.kryoserializer.buffer" -> "4",
      "spark.kryo.registrationRequired" -> "true",
      "spark.kryo.referenceTracking" -> "true",
      "spark.driver.host" -> "localhost"
    )

  for {
    (k, v) <- properties
  } {
    conf.set(k, v)
  }
}

