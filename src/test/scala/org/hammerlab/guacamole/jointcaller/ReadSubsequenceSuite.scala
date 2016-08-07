package org.hammerlab.guacamole.jointcaller

import org.hammerlab.guacamole.jointcaller.pileup_summarization.ReadSubsequence
import org.hammerlab.guacamole.pileup.{Util => PileupUtil}
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}


class ReadSubsequenceSuite
  extends GuacFunSuite
    with ReadsUtil
    with PileupUtil {

  val cancerWGS1Bams = Vector("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  implicit lazy val reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "NTCGATCGACG")))

  val partialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def partialReference = {
    ReferenceBroadcast(partialFasta, sc, partialFasta = true)
  }

  test("ofFixedReferenceLength") {
    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),  // no variant
        ("TCGACCCTCGA", "4M3I4M", 1),  // insertion
        ("TCGAGCGA", "8M", 1),  // snv
        ("TNGAGCGA", "8M", 1)  // contains N base
      )

    val pileups = reads.map(read => makePileup(Seq(read), "chr1", 1))

    ReadSubsequence.ofFixedReferenceLength(pileups(0).elements.head, 1).get.sequence should equal("C")
    ReadSubsequence.ofFixedReferenceLength(pileups(0).elements.head, 2).get.sequence should equal("CG")
    ReadSubsequence.ofFixedReferenceLength(pileups(1).elements.head, 6).get.sequence should equal("CGACCCTCG")
    ReadSubsequence.ofFixedReferenceLength(pileups(3).elements.head, 1).get.sequenceIsAllStandardBases should equal(false)
  }

  test("ofNextAltAllele") {
    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),  // no variant
        ("TCGAGCGA", "8M", 1),  // snv
        ("TCGACCCTCGA", "4M3I4M", 1),  // insertion
        ("TCGGCCCTCGA", "4M3I4M", 1),  // insertion
        ("TCAGCCCTCGA", "4M3I4M", 1),  // insertion
        ("TNGAGCGA", "8M", 1)  // contains N
      )

    val pileups = reads.map(read => makePileup(Seq(read), "chr1", 1))

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

  test("gathering possible alleles") {
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
