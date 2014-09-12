package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.guacamole.Bases

/**
 *
 * A variant that exists in a tumor sample, but not in the normal sample; includes supporting read statistics from both samples
 *
 * @param sampleName sample the variant was called on
 * @param referenceContig chromosome or genome contig of the variant
 * @param start start position of the variant (0-based)
 * @param referenceBase base in the reference genome
 * @param alternateBases base in the sample genome
 * @param somaticLogOdds log odds-ratio of the variant in the tumor compared to the normal sample
 * @param tumorEvidence supporting statistics for the variant in the tumor sample
 * @param normalEvidence supporting statistics for the variant in the normal sample
 * @param length length of the variant
 */
case class CalledSomaticGenotype(sampleName: String,
                                 referenceContig: String,
                                 start: Long,
                                 referenceBase: Byte,
                                 alternateBases: Seq[Byte],
                                 somaticLogOdds: Double,
                                 tumorEvidence: GenotypeEvidence,
                                 normalEvidence: GenotypeEvidence,
                                 length: Int = 1) extends ReferenceVariant {
  val end: Long = start + 1L

  // P ( variant in tumor AND no variant in normal) = P(variant in tumor) * ( 1 - P(variant in normal) )
  lazy val phredScaledSomaticLikelihood =
    PhredUtils.successProbabilityToPhred(tumorEvidence.likelihood * (1 - normalEvidence.likelihood) - 1e-10)
}

class CalledSomaticGenotypeSerializer extends Serializer[CalledSomaticGenotype] {

  lazy val genotypeEvidenceSerializer = new GenotypeEvidenceSerializer()

  def write(kryo: Kryo, output: Output, obj: CalledSomaticGenotype) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start)
    output.writeByte(obj.referenceBase)
    output.writeInt(obj.alternateBases.length, true)
    output.writeBytes(obj.alternateBases.toArray)
    output.writeDouble(obj.somaticLogOdds)

    genotypeEvidenceSerializer.write(kryo, output, obj.tumorEvidence)
    genotypeEvidenceSerializer.write(kryo, output, obj.normalEvidence)

    output.writeInt(obj.length, true)

  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledSomaticGenotype]): CalledSomaticGenotype = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong()
    val referenceBase: Byte = input.readByte()
    val alternateLength = input.readInt(true)
    val alternateBases = input.readBytes(alternateLength).toSeq
    val somaticLogOdds = input.readDouble()

    val tumorEvidence = genotypeEvidenceSerializer.read(kryo, input, classOf[GenotypeEvidence])
    val normalEvidence = genotypeEvidenceSerializer.read(kryo, input, classOf[GenotypeEvidence])

    val length: Int = input.readInt(true)

    CalledSomaticGenotype(
      sampleName,
      referenceContig,
      start,
      referenceBase,
      alternateBases,
      somaticLogOdds,
      tumorEvidence = tumorEvidence,
      normalEvidence = normalEvidence
    )

  }

}
