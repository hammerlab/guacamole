package org.hammerlab.guacamole.commands.jointcaller

import java.util

import htsjdk.variant.variantcontext.GenotypeBuilder
import htsjdk.variant.vcf.{ VCFFilterHeaderLine, VCFFormatHeaderLine, VCFHeaderLine, VCFHeaderLineType }
import org.hammerlab.guacamole.filters.FishersExactTest

/**
 * Extra information, such as filters, we compute about a potential call.
 *
 * See AlleleEvidenceAcrossSamplesAnnotation for more info on annotations.
 */
trait SampleAlleleEvidenceAnnotation {
  val parameters: Parameters

  /** is this annotation a failing filter? */
  def isFiltered: Boolean = false

  /** update VCF with annotation fields */
  def addInfoToVCF(builder: GenotypeBuilder): Unit
}

object SampleAlleleEvidenceAnnotation {
  type NamedAnnotations = Map[String, SampleAlleleEvidenceAnnotation]
  val emptyAnnotations = Map[String, SampleAlleleEvidenceAnnotation]()

  val availableAnnotations: Seq[Metadata] = Seq(StrandBias)

  /** A factory for the annotation. */
  trait Metadata {
    val name: String
    def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit
    def apply(stats: PileupStats, evidence: SampleAlleleEvidence, parameters: Parameters): SampleAlleleEvidenceAnnotation
  }

  /**
   * return a new SampleAlleleEvidence with all available annotations computed for it
   */
  def annotate(stats: PileupStats, evidence: SampleAlleleEvidence, parameters: Parameters): SampleAlleleEvidence = {
    val namedAnnotations = availableAnnotations.map(
      annotation => (annotation.name, annotation(stats, evidence, parameters))).toMap
    evidence.withAnnotations(namedAnnotations)
  }

  /** setup headers for fields written out by annotations in their addInfoToVCF methods. */
  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    availableAnnotations.foreach(annotation => annotation.addVCFHeaders(headerLines))
  }

  /**
   * Strand bias annotation and filter
   *
   * @param parameters
   * @param variantForward number of variant reads on + strand
   * @param variantReverse number of variant reads on - strand
   * @param totalForward total reads on + strand
   * @param totalReverse total reads on - strand
   */
  case class StrandBias(
      parameters: Parameters,
      variantForward: Int,
      variantReverse: Int,
      totalForward: Int,
      totalReverse: Int) extends SampleAlleleEvidenceAnnotation {

    val phredValue = 10.0 * FishersExactTest.asLog10(totalForward, totalReverse, variantForward, variantReverse)

    override val isFiltered = phredValue > parameters.filterStrandBiasPhred

    def addInfoToVCF(builder: GenotypeBuilder): Unit = {
      builder.attribute("FS", phredValue.round.toInt)
    }
  }
  object StrandBias extends Metadata {
    val name = "STRAND_BIAS"
    def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
      headerLines.add(new VCFFormatHeaderLine("FS", 1, VCFHeaderLineType.Integer, "Phred scaled strand bias"))
      headerLines.add(new VCFFilterHeaderLine(name, "Phred scaled strand bias (FS) exceeds threshold"))
    }

    def apply(stats: PileupStats, evidence: SampleAlleleEvidence, parameters: Parameters): StrandBias = {
      val variantReads = stats.alleleToSubsequences.getOrElse(evidence.allele.alt, Seq.empty)
      val variantForward = variantReads.count(_.read.isPositiveStrand)
      val totalForward = stats.subsequences.count(_.read.isPositiveStrand)
      StrandBias(
        parameters,
        variantForward = variantForward,
        variantReverse = variantReads.length - variantForward,
        totalForward = totalForward,
        totalReverse = stats.subsequences.length - totalForward
      )
    }
  }
}
