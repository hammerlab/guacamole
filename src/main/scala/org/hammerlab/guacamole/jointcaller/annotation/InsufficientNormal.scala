package org.hammerlab.guacamole.jointcaller.annotation

import java.util

import htsjdk.variant.vcf.{ VCFFilterHeaderLine, VCFHeaderLine }
import org.hammerlab.guacamole.jointcaller.Parameters
import MultiSampleAnnotations.Annotation
import org.hammerlab.guacamole.jointcaller.evidence.MultiSampleSingleAlleleEvidence
import org.hammerlab.guacamole.jointcaller.pileup_summarization.MultiplePileupStats

/**
 * Annotation and filter for insufficient normal evidence to make a somatic variant call.
 *
 * There are many times when we have too little variant evidence in the normal to make a germline call
 * but too much to comfortably make a somatic call. This filter removes such calls.
 *
 * @param parameters
 * @param referenceReads number of reads matching reference allele in pooled normal dna
 * @param totalReads total number of reads in pooled normal dna
 */
case class InsufficientNormal(
    parameters: Parameters,
    referenceReads: Int,
    totalReads: Int) extends Annotation {

  override val isFiltered = {
    totalReads < parameters.filterSomaticNormalDepth ||
      (totalReads - referenceReads) * 100.0 / totalReads > parameters.filterSomaticNormalNonreferencePercent
  }
  override val name: String = InsufficientNormal.name
}
object InsufficientNormal {
  val name = "INSUFFICIENT_NORMAL"
  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    headerLines.add(new VCFFilterHeaderLine(name, "Insufficient normal evidence to make call"))
  }

  def apply(
    allStats: MultiplePileupStats,
    evidence: MultiSampleSingleAlleleEvidence,
    parameters: Parameters): Option[InsufficientNormal] = {
    if (evidence.isGermlineCall) {
      None
    } else {
      val stats = allStats.normalDNAPooled
      val referenceReads = stats.allelicDepths.getOrElse(evidence.allele.ref, 0)
      Some(
        InsufficientNormal(
          parameters,
          referenceReads = referenceReads,
          totalReads = stats.totalDepthIncludingReadsContributingNoAlleles))
    }
  }
}
