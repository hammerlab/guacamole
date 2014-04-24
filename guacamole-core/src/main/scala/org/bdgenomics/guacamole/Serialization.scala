package org.bdgenomics.guacamole

import com.esotericsoftware.kryo.Kryo
import org.bdgenomics.adam.serialization.{ ADAMKryoProperties, ADAMKryoRegistrator }

object Serialization {
  /**
   * Sets up serialization properties for ADAM and Guacamole.
   *
   * @param kryoBufferSize Buffer size in MB to allocate for object serialization. Default is 4MB.
   */
  def setupContextProperties(kryoBufferSize: Int = 4) = {
    ADAMKryoProperties.setupContextProperties(kryoBufferSize)
    System.setProperty("spark.kryo.registrator", "org.bdgenomics.guacamole.GuacamoleKryoRegistrator")
  }
}

// For some reason Kryo can't find it if this class is moved into the SerializationUtil object above.
class GuacamoleKryoRegistrator extends ADAMKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)
    kryo.register(classOf[LociSet], new LociSetSerializer)
    kryo.register(classOf[LociSet.SingleContig], new LociSetSingleContigSerializer)
  }
}