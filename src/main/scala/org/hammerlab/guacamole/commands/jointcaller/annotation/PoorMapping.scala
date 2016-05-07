package org.hammerlab.guacamole.commands.jointcaller.annotation

import java.util

import htsjdk.variant.variantcontext.GenotypeBuilder
import htsjdk.variant.vcf.{VCFFilterHeaderLine, VCFHeaderLine}
import org.hammerlab.guacamole.commands.jointcaller.Parameters
import org.hammerlab.guacamole.commands.jointcaller.evidence.SingleSampleSingleAlleleEvidence
import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.PileupStats

/**
  * Created by eliza on 4/30/16.
  *
  * Remove false positives caused by sequence similarity in the genome, leading to misplacement of reads.
  * Two tests are used to identify such sites: (i) candidates are rejected if ≥ 50% of the reads
  * in the tumor and normal samples have a mapping quality of zero (although reads with a mapping
  * quality of zero are discarded in the short-read preprocessing (Supplementary Methods), this
  * filter reconsiders those discarded reads);
  *  // TO DO: (is the above true in our case?)
  * and (ii) candidates are rejected if they do not
  * have at least a single observation of the mutant allele with a confident mapping (that is,
  * mapping quality score ≥ 20).
  */
case class PoorMapping(zeroQualityReadsFraction: Double,
                       altAlleleConfidentMapCount: Int,
                        parameters: Parameters) extends SingleSampleAnnotations.Annotation {
  val name = PoorMapping.name

  override val isFiltered = {
    (zeroQualityReadsFraction > parameters.filterPoorMappingReadsFraction) ||
      altAlleleConfidentMapCount > 0
  }
  def addInfoToVCF(builder: GenotypeBuilder): Unit = {
    // TO DO just truncate using %
    val floored = (math floor zeroQualityReadsFraction*100) / 100
    builder.attribute("PM", s"$floored $altAlleleConfidentMapCount")
  }
}

object PoorMapping {
  val name = "POOR_MAPPING"

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    headerLines.add(new VCFFilterHeaderLine(name, "Remove false positives caused by sequence similarity in the genome, " +
      "leading to misplacement of reads."))
  }

  def apply(stats: PileupStats,
            evidence: SingleSampleSingleAlleleEvidence,
            parameters: Parameters) = {
    val subsequences = stats.subsequences

    val numReads = subsequences.length
    val numZeroQuality: Double = subsequences.count(rss => rss.read.alignmentQuality == 0)
    val fractionZero = numZeroQuality / numReads

    val allele = evidence.allele

    val alleleReadSubsequences = stats.alleleToSubsequences(allele.toString)

    val confidentMappings = alleleReadSubsequences.count(rss => {
      rss.read.alignmentQuality >= parameters.filterMinimumMappingConfidence})

    PoorMapping(fractionZero, confidentMappings, parameters)
  }

}
