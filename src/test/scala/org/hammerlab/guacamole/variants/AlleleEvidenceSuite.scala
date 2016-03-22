package org.hammerlab.guacamole.variants

import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.scalatest.Matchers

class AlleleEvidenceSuite extends GuacFunSuite with Matchers {

  def reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "NTAGATCGA")))

  sparkTest("allele evidence from pileup, all reads support") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1, alignmentQuality = 30),
      TestUtil.makeRead("TCGATCGA", "8M", 1, alignmentQuality = 30),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1, alignmentQuality = 60))
    val variantPileup = Pileup(reads, "chr1", 2, reference.getContig("chr1"))

    val variantEvidence = AlleleEvidence(
      likelihood = 0.5,
      allele = Allele("A", "C"),
      variantPileup
    )

    variantEvidence.meanMappingQuality should be(40.0)
    variantEvidence.medianMappingQuality should be(30)
    variantEvidence.medianMismatchesPerRead should be(1)
  }

  sparkTest("allele evidence from pileup, one read supports") {
    val reads = Seq(
      TestUtil.makeRead("TAGATCGA", "8M", 1, alignmentQuality = 30),
      TestUtil.makeRead("TCGATCGA", "8M", 1, alignmentQuality = 60),
      TestUtil.makeRead("TAGACCCTCGA", "4M3I4M", 1, alignmentQuality = 60))
    val variantPileup = Pileup(reads, "chr1", 2, reference.getContig("chr1"))

    val variantEvidence = AlleleEvidence(
      likelihood = 0.5,
      allele = Allele("A", "C"),
      variantPileup
    )

    variantEvidence.meanMappingQuality should be(60.0)
    variantEvidence.medianMappingQuality should be(60)
    variantEvidence.medianMismatchesPerRead should be(1)
  }

  sparkTest("allele evidence from pileup, no read supports") {
    val reads = Seq(
      TestUtil.makeRead("TAGATCGA", "8M", 1, alignmentQuality = 30),
      TestUtil.makeRead("TAGATCGA", "8M", 1, alignmentQuality = 60),
      TestUtil.makeRead("TAGACCCTCGA", "4M3I4M", 1, alignmentQuality = 60))
    val variantPileup = Pileup(reads, "chr1", 2, reference.getContig("chr1"))

    val variantEvidence = AlleleEvidence(
      likelihood = 0.5,
      allele = Allele("A", "C"),
      variantPileup
    )

    variantEvidence.meanMappingQuality.toString should be("NaN")
    variantEvidence.medianMappingQuality.toString should be("NaN")
    variantEvidence.medianMismatchesPerRead.toString should be("NaN")
  }

}
