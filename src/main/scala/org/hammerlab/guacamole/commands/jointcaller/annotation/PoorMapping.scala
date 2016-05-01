package org.hammerlab.guacamole.commands.jointcaller.annotation

import java.util

import htsjdk.variant.vcf.VCFHeaderLine

/**
  * Created by eliza on 4/30/16.
  *
  * Remove false positives caused by sequence similarity in the genome, leading to misplacement of reads.
  * Two tests are used to identify such sites: (i) candidates are rejected if ≥ 50% of the reads
  * in the tumor and normal samples have a mapping quality of zero (although reads with a mapping
  * quality of zero are discarded in the short-read preprocessing (Supplementary Methods), this
  * filter reconsiders those discarded reads); and (ii) candidates are rejected if they do not
  * have at least a single observation of the mutant allele with a confident mapping (that is,
  * mapping quality score ≥ 20).
  */
case class PoorMapping() extends MultiSampleAnnotations.Annotation {

}

object PoorMapping {
  val name = "POOR_MAPPING"

  def addVCFHeaders(headerLines: util.Set[VCFHeaderLine]): Unit = { }

  def apply() = {

  }

}
