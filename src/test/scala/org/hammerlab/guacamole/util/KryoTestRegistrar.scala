package org.hammerlab.guacamole.util

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.guacamole.kryo.GuacamoleKryoRegistrar

class KryoTestRegistrar extends KryoRegistrator {

  def registerTestClasses(kryo: Kryo): Unit = {}

  override def registerClasses(kryo: Kryo): Unit = {
    new GuacamoleKryoRegistrar().registerClasses(kryo)
    registerTestClasses(kryo)
  }
}
