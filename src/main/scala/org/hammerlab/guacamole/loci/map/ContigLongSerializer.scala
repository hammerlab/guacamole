package org.hammerlab.guacamole.loci.map

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}

class ContigLongSerializer extends KryoSerializer[Contig[Long]] {
  def write(kryo: Kryo, output: Output, obj: Contig[Long]) = {
    output.writeString(obj.contig.toCharArray)
    output.writeLong(obj.asMap.size)
    obj.asMap.foreach({
      case (range, value) => {
        output.writeLong(range.start)
        output.writeLong(range.end)
        output.writeLong(value)
      }
    })
  }
  def read(kryo: Kryo, input: Input, klass: Class[Contig[Long]]): Contig[Long] = {
    val builder = LociMap.newBuilder[Long]()
    val contig = input.readString()
    val count = input.readLong()
    (0L until count).foreach(i => {
      val start = input.readLong()
      val end = input.readLong()
      val value = input.readLong()
      builder.put(contig, start, end, value)
    })
    builder.result.onContig(contig)
  }
}

