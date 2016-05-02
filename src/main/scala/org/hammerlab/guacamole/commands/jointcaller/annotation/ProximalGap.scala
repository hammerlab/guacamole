package org.hammerlab.guacamole.commands.jointcaller.annotation

import java.util

import htsjdk.variant.variantcontext.GenotypeBuilder
import htsjdk.variant.vcf.{VCFFilterHeaderLine, VCFHeaderLine}
import org.hammerlab.guacamole.commands.jointcaller.Parameters
import org.hammerlab.guacamole.commands.jointcaller.evidence.SingleSampleSingleAlleleEvidence
import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.{PileupStats, ReadSubsequence}
import org.hammerlab.guacamole.pileup.PileupElement
import org.hammerlab.guacamole.reference.ContigSequence

/**
  * Created by eliza on 4/30/16.
  *
  * Remove false positives caused by nearby misaligned small
  * insertion and deletion events. Reject candidate site if
  * there are ≥ 3 reads with insertions in an 11-base-pair window
  * centered on the candidate mutation or if there are ≥ 3 reads
  * with deletions in the same 11-base-pair window.
  *
  * How to tell if a read has an insertion or a deletion?
  *
  */
case class ProximalGap(insertionCount: Int, deletionCount: Int, parameters: Parameters)
  extends SingleSampleAnnotations.Annotation {
  val name = ProximalGap.name

  override val isFiltered: Boolean = {
    insertionCount >= parameters.filterMaxProximalInsertions ||
    deletionCount >= parameters.filterMaxProximalDeletions
  }

  def addInfoToVCF(builder: GenotypeBuilder): Unit = {
    builder.attribute("PG_I", insertionCount)
    builder.attribute("PG_D", deletionCount)
  }
}

object ProximalGap {
  val name = "PROXIMAL_GAP"

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    headerLines.add(new VCFFilterHeaderLine(name, "Potential false positive caused by nearby" +
      "misaligned small indels"))
  }

  def apply(stats: PileupStats,
            evidence: SingleSampleSingleAlleleEvidence,
            parameters: Parameters,
            referenceContigSequence: ContigSequence) = {

    var insertionCount = 0
    var deletionCount = 0

    /** make a new pileup element for each read
      * starting at 5 bp minus the current
      * position and then check if those are indels
      * and then step it forward one base with atGreaterLocus
      * and check again etc until you’re 5 bp ahead
      */

    val alleleStart = evidence.allele.start

    val halfWindow = (parameters.filterProximalWindow-1)/2 // 5
    val windowStart = alleleStart - halfWindow

    val subsequences: Seq[ReadSubsequence] = stats.subsequences

    // check all positions in the window except alleleStart
    val steps = List.range(0, halfWindow) ++ List.range(halfWindow+1, parameters.filterProximalWindow)
    var step = -1;
    for ( step <- steps ) {
      subsequences.map( rss => {
        val newPe = PileupElement(rss.read, windowStart + step, referenceContigSequence)
        if (newPe.isDeletion) deletionCount += 1
        else if (newPe.isInsertion) insertionCount += 1
      })
    }

    ProximalGap(insertionCount, deletionCount, parameters)
  }
}