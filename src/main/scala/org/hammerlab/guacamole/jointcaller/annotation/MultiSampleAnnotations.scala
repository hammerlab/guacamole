package org.hammerlab.guacamole.jointcaller.annotation

import java.util

import htsjdk.variant.variantcontext.VariantContextBuilder
import htsjdk.variant.vcf.VCFHeaderLine
import org.hammerlab.guacamole.jointcaller.Parameters
import org.hammerlab.guacamole.jointcaller.annotation.MultiSampleAnnotations.Annotation
import org.hammerlab.guacamole.jointcaller.evidence.{ MultiSampleMultiAlleleEvidence, MultiSampleSingleAlleleEvidence }
import org.hammerlab.guacamole.jointcaller.pileup_summarization.MultiplePileupStats

/**
 * Annotation for multiple samples at a single allele and a single site.
 *
 * We use "annotations" to compute extra statistics about a call that can get put in the VCF. This enables us to
 * break variant calling into a cheaper operation where we compute likelihoods to see if a call can be made, and then
 * later compute all the annotations to gather extra info and decide if it fails any filters.
 *
 * Annotations can be filters by implementing the optional isFiltered method. If that returns true, the call is
 * considered filtered.
 *
 */
case class MultiSampleAnnotations(insufficientNormal: Option[InsufficientNormal]) {

  def toSeq: Seq[Annotation] = insufficientNormal.toSeq

  def annotationsFailingFilters = toSeq.filter(_.isFiltered)

  def addInfoToVCF(builder: VariantContextBuilder): Unit = {
    toSeq.foreach(_.addInfoToVCF(builder))
  }
}
object MultiSampleAnnotations {
  trait Annotation {
    val name: String

    /** is this annotation a failing filter? */
    def isFiltered: Boolean = false

    /** update VCF with annotation fields */
    def addInfoToVCF(builder: VariantContextBuilder): Unit = {}
  }

  /**
   * Compute all annotations
   */
  def apply(multiAlleleEvidence: MultiSampleMultiAlleleEvidence,
            singleAlleleEvidence: MultiSampleSingleAlleleEvidence,
            allStats: MultiplePileupStats,
            parameters: Parameters): MultiSampleAnnotations = {
    MultiSampleAnnotations(
      InsufficientNormal(allStats, singleAlleleEvidence, parameters))
  }

  /** setup headers for fields written out by annotations in their addInfoToVCF methods. */
  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    InsufficientNormal.addVCFHeaders(headerLines)
  }
}
