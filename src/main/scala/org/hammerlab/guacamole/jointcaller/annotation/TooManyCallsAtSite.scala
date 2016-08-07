package org.hammerlab.guacamole.jointcaller.annotation

import java.util

import htsjdk.variant.vcf.{ VCFFilterHeaderLine, VCFHeaderLine }
import org.hammerlab.guacamole.jointcaller.Parameters
import org.hammerlab.guacamole.jointcaller.evidence.MultiSampleMultiAlleleEvidence

/**
 * Filter sites where the number of calls exceeds a threshold (usually 1).
 *
 * @param numCalls number of calls at the site
 * @param parameters
 */
case class TooManyCallsAtSite(numCalls: Int,
                              parameters: Parameters) extends MultiSampleAnnotations.Annotation {

  override val name = TooManyCallsAtSite.name
  override val isFiltered = numCalls > parameters.maxCallsPerSite
}
object TooManyCallsAtSite {
  val name = "TOO_MANY_CALLS_AT_SITE"
  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = {
    headerLines.add(new VCFFilterHeaderLine(name, "Too many variants are called at this site"))
  }

  def apply(evidence: MultiSampleMultiAlleleEvidence,
            parameters: Parameters): Option[TooManyCallsAtSite] = {
    if (parameters.maxCallsPerSite == 0) {
      None
    } else {
      Some(TooManyCallsAtSite(evidence.singleAlleleEvidences.count(_.isCall), parameters))
    }
  }
}
