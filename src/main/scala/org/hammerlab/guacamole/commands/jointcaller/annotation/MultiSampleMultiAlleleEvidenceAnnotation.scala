package org.hammerlab.guacamole.commands.jointcaller.annotation

import java.util

import htsjdk.variant.vcf.{VCFFilterHeaderLine, VCFHeaderLine}
import org.hammerlab.guacamole.commands.jointcaller.evidence.MultiSampleMultiAlleleEvidence
import org.hammerlab.guacamole.commands.jointcaller.Parameters

/**
 * Annotation for multiple alleles at a single site.
 *
 * See AlleleEvidenceAcrossSamplesAnnotation for more information on annotaitons.
 *
 */
trait MultiSampleMultiAlleleEvidenceAnnotation extends MultiSampleSingleAlleleEvidenceAnnotation {}

object MultiSampleMultiAlleleEvidenceAnnotation {
  type NamedAnnotations = Map[String, MultiSampleMultiAlleleEvidenceAnnotation]
  val emptyAnnotations = Map[String, MultiSampleMultiAlleleEvidenceAnnotation]()

  val availableAnnotations: Seq[Metadata] = Vector(TooManyCallsAtSite)

  /** A factory for the annotation. */
  trait Metadata {
    val name: String
    def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit
    def apply(
               evidence: MultiSampleMultiAlleleEvidence,
               parameters: Parameters): Option[MultiSampleMultiAlleleEvidenceAnnotation]
  }

  /** Return all available annotations evaluated on the given AllelesAndEvidenceAtSite instance. */
  def makeAnnotations(evidence: MultiSampleMultiAlleleEvidence, parameters: Parameters): NamedAnnotations = {
    availableAnnotations
      .map(annotation => (annotation.name, annotation(evidence, parameters)))
      .flatMap(pair => pair._2.map(value => pair._1 -> value))
      .toMap
  }

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    availableAnnotations.foreach(annotation => annotation.addVCFHeaders(headerLines))
  }

  /**
   * Filter sites where the number of calls exceeds a threshold (usually 1).
   *
   * @param numCalls number of calls at the site
   * @param parameters
   */
  case class TooManyCallsAtSite(numCalls: Int,
                                parameters: Parameters) extends MultiSampleMultiAlleleEvidenceAnnotation {
    override val isFiltered = numCalls > parameters.maxCallsPerSite
  }
  object TooManyCallsAtSite extends Metadata {
    val name = "TOO_MANY_CALLS_AT_SITE"
    def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
      headerLines.add(new VCFFilterHeaderLine(name, "Too many variants are called at this site"))
    }

    def apply(evidence: MultiSampleMultiAlleleEvidence,
              parameters: Parameters): Option[MultiSampleMultiAlleleEvidenceAnnotation] = {
      if (parameters.maxCallsPerSite == 0) {
        None
      } else {
        Some(TooManyCallsAtSite(evidence.alleleEvidences.count(_.isCall), parameters))
      }
    }
  }
}
