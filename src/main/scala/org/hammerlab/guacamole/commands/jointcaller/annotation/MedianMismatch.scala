package org.hammerlab.guacamole.commands.jointcaller.annotation

import java.util

import breeze.linalg.DenseVector
import breeze.stats.median
import htsjdk.variant.variantcontext.GenotypeBuilder
import htsjdk.variant.vcf.{VCFFilterHeaderLine, VCFHeaderLineType, VCFFormatHeaderLine, VCFHeaderLine}
import org.hammerlab.guacamole.commands.jointcaller.{AlleleAtLocus, Parameters}
import org.hammerlab.guacamole.commands.jointcaller.evidence.SingleSampleSingleAlleleEvidence
import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.PileupStats
import org.hammerlab.guacamole.pileup.PileupElement

/**
  * Median mismatches per read of variant-supporting reads
  * // TO DO: ref-supporting reads?
 *
  * @param medianMismatches
  * @param parameters
  */
case class MedianMismatch(medianMismatches: Int, parameters: Parameters)
  extends SingleSampleAnnotations.Annotation {

  val name = MedianMismatch.name

  override val isFiltered = medianMismatches > parameters.filterMaximumMedianMismatches

  def addInfoToVCF(builder: GenotypeBuilder) = {
    builder.attribute("MM", medianMismatches)
  }
}

object MedianMismatch {
  val name = "MEDIAN_MISMATCH"

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]) = {
    headerLines.add(new VCFFormatHeaderLine("MM", 1, VCFHeaderLineType.Integer, "Median mismatches per read" +
      "of variant-supporting reads"))
    headerLines.add(new VCFFilterHeaderLine(name, "Median mismatches per read exceeds threshold"))
  }

  def apply(stats: PileupStats, evidence: SingleSampleSingleAlleleEvidence,
            parameters: Parameters): MedianMismatch = {

    val allele: AlleleAtLocus = evidence.allele
    val alleleElements: Seq[PileupElement] = stats.elements.filter(_.allele == allele)

    val medianMismatchesPerRead: Int = median(
      DenseVector(
        alleleElements.map( pileupElement =>
          pileupElement.read.countOfMismatches(pileupElement.referenceContigSequence)).toArray))

    MedianMismatch(medianMismatchesPerRead, parameters)
  }
}
