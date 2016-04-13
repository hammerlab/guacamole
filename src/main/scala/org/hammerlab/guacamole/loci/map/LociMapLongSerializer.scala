package org.hammerlab.guacamole.loci.map

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}

// Serialization: currently only support LociMap[Long].
class LociMapLongSerializer extends KryoSerializer[LociMap[Long]] {
  def write(kryo: Kryo, output: Output, obj: LociMap[Long]) = {
    output.writeLong(obj.contigs.length)
    obj.contigs.foreach(contig => {
      kryo.writeObject(output, obj.onContig(contig))
    })
  }
  def read(kryo: Kryo, input: Input, klass: Class[LociMap[Long]]): LociMap[Long] = {
    val count: Long = input.readLong()
    val pairs = (0L until count).map(i => {
      val obj = kryo.readObject(input, classOf[Contig[Long]])
      obj.contig -> obj
    })
    LociMap[Long](Map[String, Contig[Long]](pairs: _*))
  }
}
