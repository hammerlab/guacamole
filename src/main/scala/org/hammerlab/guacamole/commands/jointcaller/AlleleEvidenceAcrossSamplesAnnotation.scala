package org.hammerlab.guacamole.commands.jointcaller

import java.util

import htsjdk.variant.variantcontext.VariantContextBuilder
import htsjdk.variant.vcf.{ VCFFilterHeaderLine, VCFHeaderLine }

trait AlleleEvidenceAcrossSamplesAnnotation {
  def isFiltered: Boolean = false
  def addInfoToVCF(builder: VariantContextBuilder): Unit = {}
}
object AlleleEvidenceAcrossSamplesAnnotation {
  type NamedAnnotations = Map[String, AlleleEvidenceAcrossSamplesAnnotation]
  val emptyAnnotations = Map[String, AlleleEvidenceAcrossSamplesAnnotation]()

  val availableAnnotations: Seq[Metadata] = Vector(InsufficientNormal)

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
