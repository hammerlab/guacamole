package org.hammerlab.guacamole.loci.map

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import org.hammerlab.guacamole.reference.ContigName

/**
 * We serialize a LociMap simply by writing out all of its Contigs.
 */
class Serializer[T] extends KryoSerializer[LociMap[T]] {
  def write(kryo: Kryo, output: Output, obj: LociMap[T]) = {
    output.writeLong(obj.contigs.size)
    obj.contigs.foreach(contig => {
      kryo.writeObject(output, contig)
    })
  }

  def read(kryo: Kryo, input: Input, klass: Class[LociMap[T]]): LociMap[T] = {
    val count: Long = input.readLong()
    val contigs = (0L until count).map(i => {
      kryo.readObject(input, classOf[Contig[T]])
    })
    LociMap.fromContigs(contigs)
  }
}

