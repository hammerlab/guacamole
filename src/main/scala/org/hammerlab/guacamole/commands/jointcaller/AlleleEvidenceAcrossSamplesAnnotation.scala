package org.hammerlab.guacamole.commands.jointcaller

import java.util

import htsjdk.variant.variantcontext.VariantContextBuilder
import htsjdk.variant.vcf.{ VCFFilterHeaderLine, VCFHeaderLine }

/**
 * Annotation for multiple samples at a single allele and site.
 *
 * We use "annotations" to compute extra statistics about a call that can get put in the VCF. This enables us to
 * break variant calling into a cheaper operation where we compute likelihoods to see if a call can be made, and then
 * later compute all the annotations to gather extra info and decide if it fails any filters.
 *
 * Annotations can be filters by implementing the optional isFiltered method. If that returns true, the call is
 * considered filtered.
 *
 * There are 3 kinds of annotations based on what kind of input they look at:
 *   - Single allele, single sample (SampleAlleleEvidenceAnnotation)
 *   - Single allele, multiple samples (this class)
 *   - Multiple alleles, multiple samples (AllelesAndEvidenceAtSiteAnnotation)
 *
 */
trait AlleleEvidenceAcrossSamplesAnnotation {
  /** is this annotation a failing filter? */
  def isFiltered: Boolean = false

  /** update VCF with annotation fields */
  def addInfoToVCF(builder: VariantContextBuilder): Unit = {}
}
object AlleleEvidenceAcrossSamplesAnnotation {
  type NamedAnnotations = Map[String, AlleleEvidenceAcrossSamplesAnnotation]
  val emptyAnnotations = Map[String, AlleleEvidenceAcrossSamplesAnnotation]()

  val availableAnnotations: Seq[Metadata] = Vector(InsufficientNormal)

  /** A factory for the annotation. */
  trait Metadata {
    val name: String
    def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit
    def apply(
      allStats: MultiplePileupStats,
      evidence: AlleleEvidenceAcrossSamples,
      parameters: Parameters): Option[AlleleEvidenceAcrossSamplesAnnotation]
  }

  def makeAnnotations(
    allStats: MultiplePileupStats,
    evidence: AlleleEvidenceAcrossSamples,
    parameters: Parameters): NamedAnnotations = {

    availableAnnotations
      .map(annotation => (annotation.name, annotation(allStats, evidence, parameters)))
      .flatMap(pair => pair._2.map(value => pair._1 -> value))
      .toMap
  }

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    availableAnnotations.foreach(annotation => annotation.addVCFHeaders(headerLines))
  }

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
      totalReads: Int) extends AlleleEvidenceAcrossSamplesAnnotation {

    override val isFiltered = {
      totalReads < parameters.filterSomaticNormalDepth ||
        (totalReads - referenceReads) * 100.0 / totalReads > parameters.filterSomaticNormalNonreferencePercent
    }
  }
  object InsufficientNormal extends Metadata {
    val name = "INSUFFICIENT_NORMAL"
    def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
      headerLines.add(new VCFFilterHeaderLine(name, "Insufficient normal evidence to make call"))
    }

    def apply(
      allStats: MultiplePileupStats,
      evidence: AlleleEvidenceAcrossSamples,
      parameters: Parameters): Option[AlleleEvidenceAcrossSamplesAnnotation] = {
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
}
