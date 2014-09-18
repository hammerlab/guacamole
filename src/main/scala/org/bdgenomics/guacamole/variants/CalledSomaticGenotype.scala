package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.adam.util.PhredUtils

/**
 *
 * A variant that exists in a tumor sample, but not in the normal sample; includes supporting read statistics from both samples
 *
 * @param sampleName sample the variant was called on
 * @param referenceContig chromosome or genome contig of the variant
 * @param start start position of the variant (0-based)
 * @param allele reference and sequence bases of this variant
 * @param somaticLogOdds log odds-ratio of the variant in the tumor compared to the normal sample
 * @param tumorEvidence supporting statistics for the variant in the tumor sample
 * @param normalEvidence supporting statistics for the variant in the normal sample
 * @param length length of the variant
 */
case class CalledSomaticGenotype(sampleName: String,
                                 referenceContig: String,
                                 start: Long,
                                 allele: Allele,
                                 somaticLogOdds: Double,
                                 tumorEvidence: AlleleEvidence,
                                 normalEvidence: AlleleEvidence,
                                 length: Int = 1) extends ReferenceVariant {
  val end: Long = start + 1L

  // P ( variant in tumor AND no variant in normal) = P(variant in tumor) * ( 1 - P(variant in normal) )
  lazy val phredScaledSomaticLikelihood =
    PhredUtils.successProbabilityToPhred(tumorEvidence.likelihood * (1 - normalEvidence.likelihood) - 1e-10)
}

class CalledSomaticGenotypeSerializer
    extends Serializer[CalledSomaticGenotype]
    with HasGenotypeEvidenceSerializer
    with HasAlleleSerializer {

  def write(kryo: Kryo, output: Output, obj: CalledSomaticGenotype) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start)
    alleleSerializer.write(kryo, output, obj.allele)
    output.writeDouble(obj.somaticLogOdds)

    alleleEvidenceSerializer.write(kryo, output, obj.tumorEvidence)
    alleleEvidenceSerializer.write(kryo, output, obj.normalEvidence)

    output.writeInt(obj.length, true)

  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledSomaticGenotype]): CalledSomaticGenotype = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong()
    val allele: Allele = alleleSerializer.read(kryo, input, classOf[Allele])
    val somaticLogOdds = input.readDouble()

    val tumorEvidence = alleleEvidenceSerializer.read(kryo, input, classOf[AlleleEvidence])
    val normalEvidence = alleleEvidenceSerializer.read(kryo, input, classOf[AlleleEvidence])

    val length: Int = input.readInt(true)

    CalledSomaticGenotype(
      sampleName,
      referenceContig,
      start,
      allele,
      somaticLogOdds,
      tumorEvidence = tumorEvidence,
      normalEvidence = normalEvidence
    )

  }

}
