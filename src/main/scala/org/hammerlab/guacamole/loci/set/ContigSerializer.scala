package org.hammerlab.guacamole.loci.set

import java.lang.{Long => JLong}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import com.google.common.collect.{TreeRangeSet, Range => JRange}
import org.hammerlab.guacamole.loci.SimpleRange

// We serialize a LociSet simply by writing out its constituent Contigs.
class ContigSerializer extends KryoSerializer[Contig] {
  def write(kryo: Kryo, output: Output, obj: Contig) = {
    output.writeString(obj.name)
    kryo.writeObject(output, obj.ranges)
  }

  def read(kryo: Kryo, input: Input, klass: Class[Contig]): Contig = {
    val name = input.readString()
    val ranges = kryo.readObject(input, classOf[Array[SimpleRange]])
    val treeRangeSet = TreeRangeSet.create[JLong]()
    for {
      SimpleRange(start, end) <- ranges
    } {
      treeRangeSet.add(JRange.closedOpen(start, end))
    }
    Contig(name, treeRangeSet)
  }
}
