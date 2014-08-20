package org.bdgenomics.guacamole

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.formats.avro.{ ADAMVariant, ADAMContig, ADAMGenotype, ADAMGenotypeAllele }
import scala.collection.JavaConversions
import org.bdgenomics.adam.util.PhredUtils

/**
 * A Genotype is a sequence of alleles of length equal to the ploidy of the organism.
 *
 * A Genotype is for a particular reference locus. Each allele gives the base(s) present on a chromosome at that
 * locus.
 *
 * For example, the possible single-base diploid genotypes are Seq('A', 'A'), Seq('A', 'T') ... Seq('T', 'T').
 * Alleles can also be multiple bases as well, e.g. Seq("AAA", "T")
 *
 */
case class Genotype(alleles: Seq[Byte]*) extends Serializable {

  /**
   * The ploidy of the organism is the number of alleles in the genotype.
   */
  val ploidy = alleles.size

  lazy val uniqueAllelesCount = alleles.toSet.size

  def getNonReferenceAlleles(referenceAllele: Byte): Seq[Seq[Byte]] = {
    alleles.filter(allele => allele.length != 1 || allele(0) != referenceAllele)
  }

  /**
   * Counts alleles in this genotype that are not the same as the specified reference allele.
   *
   * @param referenceAllele Reference allele to compare against
   * @return Count of non reference alleles
   */
  def numberOfVariants(referenceAllele: Byte): Int = {
    getNonReferenceAlleles(referenceAllele).size
  }

  /**
   * Returns whether this genotype contains any non-reference alleles for a given reference sequence.
   *
   * @param referenceAllele Reference allele to compare against
   * @return True if at least one allele is not the reference
   */
  def isVariant(referenceAllele: Byte): Boolean = {
    numberOfVariants(referenceAllele) > 0
  }

  /**
   * Transform the alleles in this genotype to the ADAM allele enumeration.
   * Classifies alleles as Reference or Alternate.
   *
   * @param referenceAllele Reference allele to compare against
   * @return Sequence of GenotypeAlleles which are Ref, Alt or OtherAlt.
   */
  def getGenotypeAlleles(referenceAllele: Byte): Seq[ADAMGenotypeAllele] = {
    assume(ploidy == 2)
    val numVariants = numberOfVariants(referenceAllele)
    if (numVariants == 0) {
      Seq(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)
    } else if (numVariants > 0 && uniqueAllelesCount == 1) {
      Seq(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)
    } else if (numVariants >= 2 && uniqueAllelesCount > 1) {
      Seq(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.OtherAlt)
    } else {
      Seq(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)
    }
  }

}

class GenotypeSerializer extends Serializer[Genotype] {
  def write(kryo: Kryo, output: Output, obj: Genotype) = {
    output.writeInt(obj.ploidy)
    obj.alleles.foreach(allele => {
      output.writeInt(allele.length)
      output.writeBytes(allele.toArray)
    })
  }

  def read(kryo: Kryo, input: Input, klass: Class[Genotype]): Genotype = {

    val ploidy = input.readInt()
    val alleles: Seq[Seq[Byte]] = (0 until ploidy).map(i => {
      val length = input.readInt(true)
      input.readBytes(length).toSeq
    })

    Genotype(alleles: _*)

  }
}

object CalledGenotype {

  implicit def calledGenotypeToADAMGenotype(calledGenotype: CalledGenotype): Seq[ADAMGenotype] = {
    val variant = ADAMVariant.newBuilder
      .setPosition(calledGenotype.start)
      .setReferenceAllele(Bases.baseToString(calledGenotype.referenceBase))
      .setVariantAllele(Bases.basesToString(calledGenotype.alternateBase))
      .setContig(ADAMContig.newBuilder.setContigName(calledGenotype.referenceContig).build)
      .build
    Seq(ADAMGenotype.newBuilder
      .setAlleles(JavaConversions.seqAsJavaList(Seq(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)))
      .setSampleId(calledGenotype.sampleName.toCharArray)
      .setGenotypeQuality(calledGenotype.evidence.phredScaledLikelihood)
      .setReadDepth(calledGenotype.evidence.readDepth)
      .setExpectedAlleleDosage(calledGenotype.evidence.alternateReadDepth.toFloat / calledGenotype.evidence.readDepth)
      .setAlternateReadDepth(calledGenotype.evidence.alternateReadDepth)
      .setVariant(variant)
      .build)
  }

  implicit def calledSomaticGenotypeToADAMGenotype(calledGenotype: CalledSomaticGenotype): Seq[ADAMGenotype] = {
    val variant = ADAMVariant.newBuilder
      .setPosition(calledGenotype.start)
      .setReferenceAllele(Bases.baseToString(calledGenotype.referenceBase))
      .setVariantAllele(Bases.basesToString(calledGenotype.alternateBase))
      .setContig(ADAMContig.newBuilder.setContigName(calledGenotype.referenceContig).build)
      .build
    Seq(ADAMGenotype.newBuilder
      .setAlleles(JavaConversions.seqAsJavaList(Seq(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)))
      .setSampleId(calledGenotype.sampleName.toCharArray)
      .setGenotypeQuality(calledGenotype.tumorEvidence.phredScaledLikelihood)
      .setReadDepth(calledGenotype.tumorEvidence.readDepth)
      .setExpectedAlleleDosage(calledGenotype.tumorEvidence.alternateReadDepth.toFloat / calledGenotype.tumorEvidence.readDepth)
      .setAlternateReadDepth(calledGenotype.tumorEvidence.alternateReadDepth)
      .setVariant(variant)
      .build)
  }
}

case class CalledGenotype(sampleName: String,
                          referenceContig: String,
                          start: Long,
                          referenceBase: Byte,
                          alternateBase: Seq[Byte],
                          evidence: GenotypeEvidence,
                          length: Int = 1) extends HasReferenceRegion {
  val end: Long = start + 1L
}

class CalledGenotypeSerializer extends Serializer[CalledGenotype] {

  val genotypeEvidenceSeralizer = new GenotypeEvidenceSerializer()

  def write(kryo: Kryo, output: Output, obj: CalledGenotype) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start, true)
    output.writeByte(obj.referenceBase)
    output.writeInt(obj.alternateBase.length, true)
    output.writeBytes(obj.alternateBase.toArray)
    genotypeEvidenceSeralizer.write(kryo, output, obj.evidence)
    output.writeInt(obj.length, true)

  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledGenotype]): CalledGenotype = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong(true)
    val referenceBase: Byte = input.readByte()
    val alternateLength = input.readInt(true)
    val alternateBase: Seq[Byte] = input.readBytes(alternateLength)

    val evidence = genotypeEvidenceSeralizer.read(kryo, input, classOf[GenotypeEvidence])
    val length: Int = input.readInt(true)

    CalledGenotype(
      sampleName,
      referenceContig,
      start,
      referenceBase,
      alternateBase,
      evidence,
      length
    )

  }
}

case class CalledSomaticGenotype(sampleName: String,
                                 referenceContig: String,
                                 start: Long,
                                 referenceBase: Byte,
                                 alternateBase: Seq[Byte],
                                 somaticLogOdds: Double,
                                 tumorEvidence: GenotypeEvidence,
                                 normalEvidence: GenotypeEvidence) extends HasReferenceRegion {
  val end: Long = start + 1L
}

class CalledSomaticGenotypeSerializer extends Serializer[CalledSomaticGenotype] {

  def write(kryo: Kryo, output: Output, obj: CalledSomaticGenotype) = {
    output.writeString(obj.sampleName)
    output.writeString(obj.referenceContig)
    output.writeLong(obj.start)
    output.writeByte(obj.referenceBase)
    output.writeInt(obj.alternateBase.length, true)
    output.writeBytes(obj.alternateBase.toArray)
    output.writeDouble(obj.somaticLogOdds)
    writeEvidence(output, obj.tumorEvidence)
    writeEvidence(output, obj.normalEvidence)

  }

  def read(kryo: Kryo, input: Input, klass: Class[CalledSomaticGenotype]): CalledSomaticGenotype = {

    val sampleName: String = input.readString()
    val referenceContig: String = input.readString()
    val start: Long = input.readLong()
    val referenceBase: Byte = input.readByte()
    val alternateLength = input.readInt(true)
    val alternateBase = input.readBytes(alternateLength).toSeq
    val somaticLogOdds = input.readDouble()
    val tumorEvidence = readEvidence(input)
    val normalEvidence = readEvidence(input)

    CalledSomaticGenotype(
      sampleName,
      referenceContig,
      start,
      referenceBase,
      alternateBase,
      somaticLogOdds,
      tumorEvidence = tumorEvidence,
      normalEvidence = normalEvidence
    )

  }

  def writeEvidence(output: Output, evidence: GenotypeEvidence) = {
    output.writeDouble(evidence.likelihood)
    output.writeInt(evidence.readDepth)
    output.writeInt(evidence.alternateReadDepth)
    output.writeInt(evidence.forwardDepth)
    output.writeInt(evidence.alternateForwardDepth)
  }

  def readEvidence(input: Input): GenotypeEvidence = {
    val likelihood = input.readDouble()
    val readDepth = input.readInt()
    val alternateReadDepth = input.readInt()
    val forwardDepth = input.readInt()
    val alternateForwardDepth = input.readInt()

    GenotypeEvidence(likelihood,
      readDepth,
      alternateReadDepth,
      forwardDepth,
      alternateForwardDepth)

  }
}

case class GenotypeEvidence(likelihood: Double,
                            readDepth: Int,
                            alternateReadDepth: Int,
                            forwardDepth: Int,
                            alternateForwardDepth: Int) {

  lazy val phredScaledLikelihood = PhredUtils.successProbabilityToPhred(likelihood - 1e-10)
}

class GenotypeEvidenceSerializer extends Serializer[GenotypeEvidence] {
  def write(kryo: Kryo, output: Output, obj: GenotypeEvidence) = {
    output.writeDouble(obj.likelihood)
    output.writeInt(obj.readDepth)
    output.writeInt(obj.alternateReadDepth)
    output.writeInt(obj.forwardDepth)
    output.writeInt(obj.alternateForwardDepth)

  }

  def read(kryo: Kryo, input: Input, klass: Class[GenotypeEvidence]): GenotypeEvidence = {

    val likelihood = input.readDouble()
    val readDepth = input.readInt()
    val alternateReadDepth = input.readInt()
    val forwardDepth = input.readInt()
    val alternateForwardDepth = input.readInt()

    GenotypeEvidence(likelihood,
      readDepth,
      alternateReadDepth,
      forwardDepth,
      alternateForwardDepth)

  }
}
