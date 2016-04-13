package org.hammerlab.guacamole.loci.map

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}

import scala.collection.immutable.TreeMap

// Serialization: currently only support LociMap[Long].
class Serializer[T] extends KryoSerializer[LociMap[T]] {
  def write(kryo: Kryo, output: Output, obj: LociMap[T]) = {
    output.writeLong(obj.contigs.size)
    obj.contigs.foreach(contig => {
      kryo.writeObject(output, contig)
    })
  }

  def read(kryo: Kryo, input: Input, klass: Class[LociMap[T]]): LociMap[T] = {
    val count: Long = input.readLong()
    val pairs = (0L until count).map(i => {
      val obj = kryo.readObject(input, classOf[Contig[T]])
      obj.name -> obj
    })
    LociMap[T](TreeMap[String, Contig[T]](pairs: _*))
  }
}

