package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }

/**
 *
 * A variant that exists in the sample; includes supporting read statistics
 *
 * @param sampleName sample the variant was called on
 * @param referenceContig chromosome or genome contig of the variant
 * @param start start position of the variant (0-based)
 * @param allele allele (ref + seq bases) for this variant
 * @param evidence supporting statistics for the variant
 * @param length length of the variant
 */
case class CalledGenotype(sampleName: String,
                          referenceContig: String,
                          start: Long,
                          allele: Allele,
                          evidence: GenotypeEvidence,
                          length: Int = 1) extends ReferenceVariant {
  val end: Long = start + 1L

}

class CalledGenotypeSerializer
    extends Serializer[CalledGenotype]
    with HasAlleleSerializer
    with HasGenotypeEvidenceSerializer {

  def write(kryo: Kryo, output: Output, obj: CalledGenotype) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start, true)
    alleleSerializer.write(kryo, output, obj.allele)
    genotypeEvidenceSerializer.write(kryo, output, obj.evidence)
    output.writeInt(obj.length, true)
  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledGenotype]): CalledGenotype = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong(true)
    val allele = alleleSerializer.read(kryo, input, classOf[Allele])
    val evidence = genotypeEvidenceSerializer.read(kryo, input, classOf[GenotypeEvidence])
    val length: Int = input.readInt(true)

    CalledGenotype(
      sampleName,
      referenceContig,
      start,
      allele,
      evidence,
      length
    )

  }
}

