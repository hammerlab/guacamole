package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAllele }

import scala.collection.JavaConversions


/**
 *
 * A variant that exists in the sample; includes supporting read statistics
 *
 * @param sampleName sample the variant was called on
 * @param referenceContig chromosome or genome contig of the variant
 * @param start start position of the variant (0-based)
 * @param referenceBase base in the reference genome
 * @param alternateBases base in the sample genome
 * @param evidence supporting statistics for the variant
 * @param length length of the variant
 */
case class CalledGenotype(sampleName: String,
                          referenceContig: String,
                          start: Long,
                          referenceBase: Byte,
                          alternateBases: Seq[Byte],
                          evidence: GenotypeEvidence,
                          length: Int = 1) extends ReferenceVariant {
  val end: Long = start + 1L

}

class CalledGenotypeSerializer extends Serializer[CalledGenotype] {

  lazy val genotypeEvidenceSeralizer = new GenotypeEvidenceSerializer()

  def write(kryo: Kryo, output: Output, obj: CalledGenotype) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start, true)
    output.writeByte(obj.referenceBase)
    output.writeInt(obj.alternateBases.length, true)
    output.writeBytes(obj.alternateBases.toArray)
    genotypeEvidenceSeralizer.write(kryo, output, obj.evidence)
    output.writeInt(obj.length, true)

  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledGenotype]): CalledGenotype = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong(true)
    val referenceBase: Byte = input.readByte()
    val alternateLength = input.readInt(true)
    val alternateBases: Seq[Byte] = input.readBytes(alternateLength)

    val evidence = genotypeEvidenceSeralizer.read(kryo, input, classOf[GenotypeEvidence])
    val length: Int = input.readInt(true)

    CalledGenotype(
      sampleName,
      referenceContig,
      start,
      referenceBase,
      alternateBases,
      evidence,
      length
    )

  }
}

