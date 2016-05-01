package org.hammerlab.guacamole.commands.jointcaller.annotation

import java.util

import htsjdk.variant.variantcontext.GenotypeBuilder
import htsjdk.variant.vcf.VCFHeaderLine
import org.hammerlab.guacamole.commands.jointcaller.Parameters
import org.hammerlab.guacamole.commands.jointcaller.annotation.SingleSampleAnnotations.Annotation
import org.hammerlab.guacamole.commands.jointcaller.evidence.SingleSampleSingleAlleleEvidence
import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.PileupStats

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
  * Method:
  * make a new pileup element for each read
  * starting at 11 bp minus the current
  * position and then check if those are indels
  * and then step it forward one base with atGreaterLocus
  * and check again etc until you’re 11 bp ahead
  *
  */
case class ProximalGap(
  parameters: Parameters) extends Annotation {
  val name = ProximalGap.name

  override val isFiltered = true

  def addInfoToVCF(builder: GenotypeBuilder): Unit = {
    val tmp: Int = 0
    builder.attribute("PG", tmp)
  }
}

object ProximalGap {
  val name = "PROXIMAL_GAP"

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {

  }

  def apply(stats: PileupStats, evidence: SingleSampleSingleAlleleEvidence) = {
    var numInsertions = 0
    var numDeletions = 0
    val halfWindow = 5 // (11-1)/2


    val pileupElements = stats.elements

    pileupElements.map(pe => pe.isInsertion )


  }
}