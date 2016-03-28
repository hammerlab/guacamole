package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.ReadSubsequence
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
import org.scalatest.Matchers

class ReadSubsequenceSuite extends GuacFunSuite with Matchers {
  val cancerWGS1Bams = Vector("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  def simpleReference = TestUtil.makeReference(sc, Seq(("chr1", 0, "NTCGATCGACG")))

  val partialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def partialReference = {
    ReferenceBroadcast(partialFasta, sc, partialFasta = true)
  }

  sparkTest("ofFixedReferenceLength") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1), // no variant
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1), // insertion
      TestUtil.makeRead("TCGAGCGA", "8M", 1), // snv
      TestUtil.makeRead("TNGAGCGA", "8M", 1) // contains N base
    )
    val pileups = reads.map(read => Pileup(Seq(read), "chr1", 1, simpleReference.getContig("chr1")))

    ReadSubsequence.ofFixedReferenceLength(pileups(0).head, 1).get.sequence should equal("C")
    ReadSubsequence.ofFixedReferenceLength(pileups(0).head, 2).get.sequence should equal("CG")
    ReadSubsequence.ofFixedReferenceLength(pileups(1).head, 6).get.sequence should equal("CGACCCTCG")
    ReadSubsequence.ofFixedReferenceLength(pileups(3).head, 1).get.sequenceIsAllStandardBases should equal(false)
  }

  sparkTest("ofNextAltAllele") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1), // no variant
      TestUtil.makeRead("TCGAGCGA", "8M", 1), // snv
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1), // insertion
      TestUtil.makeRead("TCGGCCCTCGA", "4M3I4M", 1), // insertion
      TestUtil.makeRead("TCAGCCCTCGA", "4M3I4M", 1), // insertion
      TestUtil.makeRead("TNGAGCGA", "8M", 1) // contains N
    )
    val pileups = reads.map(read => Pileup(Seq(read), "chr1", 1, simpleReference.getContig("chr1")))

    ReadSubsequence.ofNextAltAllele(pileups(0).elements(0)) should equal(None)

    ReadSubsequence.ofNextAltAllele(
      pileups(1).atGreaterLocus(4, Iterator.empty).elements(0)).get.sequence should equal("G")

    ReadSubsequence.ofNextAltAllele(
      pileups(2).atGreaterLocus(3, Iterator.empty).elements(0)).get.sequence should equal("ACCC")

    ReadSubsequence.ofNextAltAllele(
      pileups(3).atGreaterLocus(3, Iterator.empty).elements(0)).get.sequence should equal("GCCC")

    ReadSubsequence.ofNextAltAllele(
      pileups(4).atGreaterLocus(2, Iterator.empty).elements(0)).get.sequence should equal("AGCCC")

    ReadSubsequence.ofNextAltAllele(pileups(5).elements(0)).get.sequenceIsAllStandardBases should equal(false)
  }

  sparkTest("gathering possible alleles") {
    val inputs = InputCollection(cancerWGS1Bams)
    val parameters = Parameters.defaults
    val contigLocus = ("chr12", 65857039)
    val pileups = cancerWGS1Bams.map(
      path =>
        TestUtil.loadPileup(
          sc,
          path,
          maybeContig = Some(contigLocus._1),
          locus = contigLocus._2,
          reference = partialReference
        )
    )

    val subsequences = ReadSubsequence.nextAlts(pileups(1).elements)
    assert(subsequences.nonEmpty)

    val alleles = AlleleAtLocus.variantAlleles(
      (inputs.normalDNA ++ inputs.tumorDNA).map(input => pileups(input.index)),
      anyAlleleMinSupportingReads = parameters.anyAlleleMinSupportingReads,
      anyAlleleMinSupportingPercent = parameters.anyAlleleMinSupportingPercent)

    assert(alleles.nonEmpty)

    alleles.map(_.alt) should equal(Seq("C"))
    alleles.map(_.ref) should equal(Seq("G"))
  }

}
