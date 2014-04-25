package org.bdgenomics.guacamole

import com.esotericsoftware.kryo.Kryo
import org.bdgenomics.adam.serialization.{ ADAMKryoRegistrator }

class GuacamoleKryoRegistrator extends ADAMKryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)
    kryo.register(classOf[LociSet], new LociSetSerializer)
    kryo.register(classOf[LociSet.SingleContig], new LociSetSingleContigSerializer)
  }
}