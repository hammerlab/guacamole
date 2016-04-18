package org.hammerlab.guacamole.loci.set

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}

// We just serialize the underlying contigs, which contain their names which are the string keys of LociSet.map.
class Serializer extends KryoSerializer[LociSet] {
  def write(kryo: Kryo, output: Output, obj: LociSet) = {
    kryo.writeClassAndObject(output, obj.contigs)
  }

  def read(kryo: Kryo, input: Input, klass: Class[LociSet]): LociSet = {
    val contigs = kryo.readClassAndObject(input).asInstanceOf[Iterable[Contig]]
    LociSet.fromContigs(contigs)
  }
}
