package org.hammerlab.guacamole.util

import com.esotericsoftware.kryo.Kryo
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.guacamole.kryo.Registrar
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

class GuacFunSuite
  extends FunSuite
    with SharedSparkContext
    with Matchers
    with KryoRegistrator
    with SparkSerializerSuite {

  conf.setAppName("guacamole")

  // Glorified union type for String ∨ Class[_].
  trait RegisterClass {
    def clazz: Class[_]
  }
  implicit class ClassNameToRegister(className: String) extends RegisterClass {
    override def clazz: Class[_] = Class.forName(className)
  }
  implicit class ClassToRegister(val clazz: Class[_]) extends RegisterClass

  private val extraKryoRegistrations = ArrayBuffer[Class[_]]()

  // Subclasses can record extra Kryo classes to register here.
  def kryoRegister(classes: RegisterClass*): Unit =
    extraKryoRegistrations ++= classes.map(_.clazz)

  override def registerClasses(kryo: Kryo): Unit = {
    new Registrar().registerClasses(kryo)
    for { clazz <- extraKryoRegistrations } {
      kryo.register(clazz)
    }
  }

  val properties: Map[String, String] =
    Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      // Use this class as its own Kryo registrar, in order to pick up classes registered above.
      "spark.kryo.registrator" -> getClass.getCanonicalName,
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

