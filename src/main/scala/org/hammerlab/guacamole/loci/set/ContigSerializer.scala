package org.hammerlab.guacamole.loci.set

import java.lang.{Long => JLong}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import com.google.common.collect.{TreeRangeSet, Range => JRange}
import org.hammerlab.guacamole.reference.Interval

// We serialize a LociSet simply by writing out its constituent Contigs.
class ContigSerializer extends KryoSerializer[Contig] {
  def write(kryo: Kryo, output: Output, obj: Contig) = {
    output.writeString(obj.name)
    output.writeInt(obj.ranges.length)
    for {
      Interval(start, end) <- obj.ranges
    } {
      output.writeLong(start)
      output.writeLong(end)
    }
  }

  def read(kryo: Kryo, input: Input, klass: Class[Contig]): Contig = {
    val name = input.readString()
    val length = input.readInt()
    val treeRangeSet = TreeRangeSet.create[JLong]()
    val ranges = (0 until length).foreach(_ => {
      treeRangeSet.add(JRange.closedOpen(input.readLong(), input.readLong()))
    })
    Contig(name, treeRangeSet)
  }
}
