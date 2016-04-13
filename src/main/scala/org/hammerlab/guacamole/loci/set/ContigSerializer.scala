package org.hammerlab.guacamole.loci.set

import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.hammerlab.guacamole.loci.map

class ContigSerializer extends KryoSerializer[Contig] {
  def write(kryo: Kryo, output: Output, obj: Contig) = {
    kryo.writeObject(output, obj.map)
  }
  def read(kryo: Kryo, input: Input, klass: Class[Contig]): Contig = {
    Contig(kryo.readObject(input, classOf[map.Contig[Long]]))
  }
}
