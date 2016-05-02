package org.hammerlab.guacamole.commands.jointcaller.annotation

import java.util

import htsjdk.variant.variantcontext.GenotypeBuilder
import htsjdk.variant.vcf.VCFHeaderLine
import org.hammerlab.guacamole.commands.jointcaller.Parameters
import org.hammerlab.guacamole.commands.jointcaller.annotation.SingleSampleAnnotations.Annotation
import org.hammerlab.guacamole.commands.jointcaller.evidence.SingleSampleSingleAlleleEvidence
import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.PileupStats
import org.hammerlab.guacamole.reference.ContigSequence

/**
 * Extra information, such as filters, we compute about a potential call.
 *
 * See MultiSampleAnnotations for more info on annotations.
 */
case class SingleSampleAnnotations(
    strandBias: StrandBias, proximalGap: ProximalGap) {

  def toSeq: Seq[Annotation] = Seq(strandBias, proximalGap)

  def annotationsFailingFilters = toSeq.filter(_.isFiltered)

  def addInfoToVCF(builder: GenotypeBuilder): Unit = {
    toSeq.foreach(_.addInfoToVCF(builder))
  }
}
object SingleSampleAnnotations {
  trait Annotation {
    val name: String

    /** is this annotation a failing filter? */
    def isFiltered: Boolean = false

    /** update VCF with annotation fields */
    def addInfoToVCF(builder: GenotypeBuilder): Unit
  }

  /**
   * Compute all annotations
   */
  def apply(stats: PileupStats,
            evidence: SingleSampleSingleAlleleEvidence,
            parameters: Parameters,
            referenceContigSequence: ContigSequence): SingleSampleAnnotations = {
    SingleSampleAnnotations(
      StrandBias(stats, evidence, parameters),
      ProximalGap(stats, evidence, parameters, referenceContigSequence))
  }

  /** setup headers for fields written out by annotations in their addInfoToVCF methods. */
  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    StrandBias.addVCFHeaders(headerLines)
    ProximalGap.addVCFHeaders(headerLines)
  }
}
