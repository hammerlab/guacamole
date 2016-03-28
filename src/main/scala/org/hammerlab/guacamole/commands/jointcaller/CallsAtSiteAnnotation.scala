package org.hammerlab.guacamole.commands.jointcaller

import java.util

import htsjdk.variant.vcf.{VCFFilterHeaderLine, VCFHeaderLine}

trait CallsAtSiteAnnotation extends AlleleEvidenceAcrossSamplesAnnotation {}

object CallsAtSiteAnnotation {
  type NamedAnnotations = Map[String, CallsAtSiteAnnotation]
  val emptyAnnotations = Map[String, CallsAtSiteAnnotation]()

  val availableAnnotations: Seq[Metadata] = Vector(TooManyCallsAtSite)

  trait Metadata {
    val name: String
    def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit
    def apply(
      evidence: CallsAtSite,
      parameters: Parameters): Option[CallsAtSiteAnnotation]
  }

  def makeAnnotations(
    evidence: CallsAtSite,
    parameters: Parameters): NamedAnnotations = {

    availableAnnotations
      .map(annotation => (annotation.name, annotation(evidence, parameters)))
      .flatMap(pair => pair._2.map(value => pair._1 -> value))
      .toMap
  }

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    availableAnnotations.foreach(annotation => annotation.addVCFHeaders(headerLines))
  }

  case class TooManyCallsAtSite(numCalls: Int,
                                parameters: Parameters) extends CallsAtSiteAnnotation {
    override val isFiltered = numCalls > parameters.maxCallsPerSite
  }
  object TooManyCallsAtSite extends Metadata {
    val name = "TOO_MANY_CALLS_AT_SITE"
    def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
      headerLines.add(new VCFFilterHeaderLine(name, "Too many variants are called at this site"))
    }

    def apply(
      evidence: CallsAtSite,
      parameters: Parameters): Option[CallsAtSiteAnnotation] = {
      if (parameters.maxCallsPerSite == 0) {
        None
      } else {
        Some(TooManyCallsAtSite(evidence.alleleEvidences.count(_.isCall), parameters))
      }
    }
  }
}
