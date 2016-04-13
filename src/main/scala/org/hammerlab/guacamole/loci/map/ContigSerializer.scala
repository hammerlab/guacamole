package org.hammerlab.guacamole.loci.map

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}

class ContigSerializer[T] extends KryoSerializer[Contig[T]] {
  def write(kryo: Kryo, output: Output, obj: Contig[T]) = {
    output.writeString(obj.name)
    output.writeLong(obj.asMap.size)
    obj.asMap.foreach {
      case (range, value) => {
        output.writeLong(range.start)
        output.writeLong(range.end)
        kryo.writeClassAndObject(output, value)
      }
    }
  }

  def read(kryo: Kryo, input: Input, klass: Class[Contig[T]]): Contig[T] = {
    val builder = LociMap.newBuilder[T]()
    val contig = input.readString()
    val count = input.readLong()
    (0L until count).foreach(_ => {
      val start = input.readLong()
      val end = input.readLong()
      val value: T = kryo.readClassAndObject(input).asInstanceOf[T]
      builder.put(contig, start, end, value)
    })
    builder.result.onContig(contig)
  }
}

