package org.hammerlab.guacamole.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.guacamole.kryo.Registrar

trait KryoTestRegistrar extends KryoRegistrator {

  def registerTestClasses(kryo: Kryo): Unit

  override def registerClasses(kryo: Kryo): Unit = {
    new Registrar().registerClasses(kryo)
    registerTestClasses(kryo)
  }
}
