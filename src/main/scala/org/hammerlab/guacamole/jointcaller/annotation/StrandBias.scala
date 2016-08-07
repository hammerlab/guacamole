package org.hammerlab.guacamole.jointcaller.annotation

import java.util

import htsjdk.variant.variantcontext.GenotypeBuilder
import htsjdk.variant.vcf.{ VCFFilterHeaderLine, VCFFormatHeaderLine, VCFHeaderLine, VCFHeaderLineType }
import org.hammerlab.guacamole.jointcaller.Parameters
import org.hammerlab.guacamole.jointcaller.annotation.SingleSampleAnnotations.Annotation
import org.hammerlab.guacamole.jointcaller.evidence.SingleSampleSingleAlleleEvidence
import org.hammerlab.guacamole.jointcaller.pileup_summarization.PileupStats
import org.hammerlab.guacamole.filters.FishersExactTest

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
    totalReverse: Int) extends Annotation {

  val name = StrandBias.name

  val phredValue = 10.0 * FishersExactTest.asLog10(totalForward, totalReverse, variantForward, variantReverse)

  override val isFiltered = phredValue > parameters.filterStrandBiasPhred

  def addInfoToVCF(builder: GenotypeBuilder): Unit = {
    builder.attribute("FS", phredValue.round.toInt)
  }
}
object StrandBias {
  val name = "STRAND_BIAS"

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    headerLines.add(new VCFFormatHeaderLine("FS", 1, VCFHeaderLineType.Integer, "Phred scaled strand bias"))
    headerLines.add(new VCFFilterHeaderLine(name, "Phred scaled strand bias (FS) exceeds threshold"))
  }

  def apply(stats: PileupStats, evidence: SingleSampleSingleAlleleEvidence, parameters: Parameters): StrandBias = {
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
