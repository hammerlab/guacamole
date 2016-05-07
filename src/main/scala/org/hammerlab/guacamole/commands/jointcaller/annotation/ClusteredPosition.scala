package org.hammerlab.guacamole.commands.jointcaller.annotation

import java.util

import breeze.stats.median
import htsjdk.variant.variantcontext.GenotypeBuilder
import htsjdk.variant.vcf.{VCFHeaderLineType, VCFFilterHeaderLine, VCFFormatHeaderLine, VCFHeaderLine}
import org.hammerlab.guacamole.commands.jointcaller.Parameters
import org.hammerlab.guacamole.commands.jointcaller.evidence.SingleSampleSingleAlleleEvidence
import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.PileupStats
import org.hammerlab.guacamole.pileup.PileupElement

/**
  * Clustered Position strand bias annotation and filter
  * Reject false positives caused by misalignments hallmarked by the alternate alleles being clustered
  * at a consistent distance from the start or end of the read alignment. We calculate the median and
  * median absolute deviation of the distance from both the start and end of the read, and reject sites
  * that have a median ≤ 10 (near the start/end of the alignment) and a median absolute deviation ≤ 3
  * (clustered).
  *
  * @param startMedian median of allele's distance from start of read
  * @param startMedianAbsoluteDeviation median absolute deviation of allele's distance from start of read
  * @param endMedian median of allele's distance from end of read
  * @param endMedianAbsoluteDeviation median absolute deviation of allele's distance from end of read
  * @param parameters
  */
case class ClusteredPosition(startMedian: Int,
                             startMedianAbsoluteDeviation: Int,
                             endMedian: Int,
                             endMedianAbsoluteDeviation: Int,
                             parameters: Parameters)
  extends SingleSampleAnnotations.Annotation {
  val name = ClusteredPosition.name
  override val isFiltered = {
    (startMedian <= parameters.filterClusteredPositionMedian || endMedian <= parameters.filterClusteredPositionMedian) &&
      (startMedianAbsoluteDeviation <= parameters.filterClusteredPositionMedianDeviation ||
        endMedianAbsoluteDeviation <= parameters.filterClusteredPositionMedianDeviation)
  }

  def addInfoToVCF(builder: GenotypeBuilder): Unit = {
    builder.attribute("CP_M_S", startMedian)
    builder.attribute("CP_M_E", endMedian)
    builder.attribute("CP_MAD_S", startMedianAbsoluteDeviation)
    builder.attribute("CP_MAD_E", endMedianAbsoluteDeviation)
  }
}

object ClusteredPosition {
  val name = "CLUSTERED_POSITION"

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    headerLines.add(new VCFFormatHeaderLine("CP_M_S", 1,
      VCFHeaderLineType.Integer, "Allele's median distance from start of read"))
    headerLines.add(new VCFFormatHeaderLine("CP_M_S", 1,
      VCFHeaderLineType.Integer, "Allele's median distance from end of read"))
    headerLines.add(new VCFFormatHeaderLine("CP_MAD_S", 1,
      VCFHeaderLineType.Integer, "Allele's median absolute deviation of distance from start of read"))
    headerLines.add(new VCFFormatHeaderLine("CP_MAD_E", 1,
      VCFHeaderLineType.Integer, "Allele's median absolute deviation of distance from end of read"))
    headerLines.add(new VCFFilterHeaderLine(name, ""))
  }

  private def medianAndMAD(distances: Seq[Int]): (Int, Int) = {
    val distanceMedian = median(distances)
    val residuals = distances.map(distance => math.abs(distance - distanceMedian))
    val residualMedian = median(residuals)
    (distanceMedian, residualMedian)
  }

  def apply(stats: PileupStats, evidence: SingleSampleSingleAlleleEvidence, parameters: Parameters):
  ClusteredPosition = {
    val pileupElements: Seq[PileupElement] = stats.elements

    val distances: Seq[(Int, Int)] = pileupElements.map( pe => {
      val fromStart = pe.readPosition
      val fromEnd = (pe.read.end - pe.read.start) - pe.readPosition
      (fromStart, fromEnd.toInt)
    })

    val distancesFromStart = distances.map(_._1)
    val distancesFromEnd = distances.map(_._2)

    val startMedians = medianAndMAD(distancesFromStart)
    val endMedians = medianAndMAD(distancesFromEnd)

    ClusteredPosition(startMedians._1, startMedians._2, endMedians._1, endMedians._2, parameters)
  }

}
