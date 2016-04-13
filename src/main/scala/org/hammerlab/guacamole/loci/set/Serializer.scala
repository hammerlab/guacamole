package org.hammerlab.guacamole.loci.set

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import org.hammerlab.guacamole.loci.map.LociMap

// Serialization: just delegate to LociMap[Long].
class Serializer extends KryoSerializer[LociSet] {
  def write(kryo: Kryo, output: Output, obj: LociSet) = {
    kryo.writeObject(output, obj.map)
  }
  def read(kryo: Kryo, input: Input, klass: Class[LociSet]): LociSet = {
    LociSet(kryo.readObject(input, classOf[LociMap[Long]]))
  }
}

