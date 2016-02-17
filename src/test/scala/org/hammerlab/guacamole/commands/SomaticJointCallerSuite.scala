package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.commands.jointcaller._
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.reference.ReferenceBroadcast.ArrayBackedReferenceSequence
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }
import org.hammerlab.guacamole.{ Bases, LociSet }
import org.scalatest.Matchers

class SomaticJointCallerSuite extends GuacFunSuite with Matchers {
  val cancerWGS1Bams = Seq("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  val partialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def partialReference = {
    ReferenceBroadcast(partialFasta, sc, partialFasta = true)
  }

  sparkTest("read subsequence ofFixedReferenceLength") {
    val ref = ArrayBackedReferenceSequence(sc, "NTCGATCGA")
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1), // no variant
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", "8", 1), // insertion
      TestUtil.makeRead("TCGAGCGA", "8M", "8", 1) // snv
    )
    val pileups = reads.map(read => Pileup(Seq(read), "chr1", 1))

    ReadSubsequence.ofFixedReferenceLength(pileups(0).head, Bases.stringToBases("C")).get.sequence should equal("C")
    ReadSubsequence.ofFixedReferenceLength(pileups(0).head, Bases.stringToBases("CG")).get.sequence should equal("CG")
    ReadSubsequence.ofFixedReferenceLength(pileups(1).head, Bases.stringToBases("CGATCG")).get.sequence should equal("CGACCCTCG")
    ReadSubsequence.ofFixedReferenceLength(pileups(2).head, Bases.stringToBases("CGATCG")).get.sequence should equal("CGAGCG")
  }

  sparkTest("read subsequence ofNextAltAllele") {
    val ref = ArrayBackedReferenceSequence(sc, "NTCGATCGA")
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1), // no variant
      TestUtil.makeRead("TCGAGCGA", "8M", "8", 1), // snv
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", "8", 1), // insertion
      TestUtil.makeRead("TCGGCCCTCGA", "4M3I4M", "8", 1), // insertion
      TestUtil.makeRead("TCAGCCCTCGA", "4M3I4M", "8", 1) // insertion
    )
    val pileups = reads.map(read => Pileup(Seq(read), "chr1", 1))

    ReadSubsequence.ofNextAltAllele(pileups(0).elements(0), ref) should equal(None)

    ReadSubsequence.ofNextAltAllele(
      pileups(1).atGreaterLocus(4, ref(4), Iterator.empty).elements(0), ref).get.sequence should equal("G")

    ReadSubsequence.ofNextAltAllele(
      pileups(2).atGreaterLocus(3, ref(3), Iterator.empty).elements(0), ref).get.sequence should equal("ACCC")

    ReadSubsequence.ofNextAltAllele(
      pileups(3).atGreaterLocus(3, ref(3), Iterator.empty).elements(0), ref).get.sequence should equal("GCCC")

    ReadSubsequence.ofNextAltAllele(
      pileups(4).atGreaterLocus(2, ref(2), Iterator.empty).elements(0), ref).get.sequence should equal("AGCCC")

  }

  sparkTest("gathering possible alleles") {
    val inputs = InputCollection(cancerWGS1Bams)
    val parameters = Parameters.defaults
    val contigLocus = ("chr12", 65857039)
    val pileups = cancerWGS1Bams.map(
      path => TestUtil.loadPileup(sc, path, contig = Some(contigLocus._1), locus = contigLocus._2))

    val reference = partialReference
    val subsequences = ReadSubsequence.nextAlts(pileups(1).elements, reference.getContig("chr12"))
    assert(subsequences.nonEmpty)

    val alleles = AlleleAtLocus.variantAlleles(
      reference,
      (inputs.normalDNA ++ inputs.tumorDNA).map(input => pileups(input.index)),
      anyAlleleMinSupportingReads = parameters.anyAlleleMinSupportingReads,
      anyAlleleMinSupportingPercent = parameters.anyAlleleMinSupportingPercent)

    assert(alleles.nonEmpty)

    alleles.map(_.alt) should equal(Seq("C"))
    alleles.map(_.ref) should equal(Seq("G"))
  }

  sparkTest("call a variant") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociSet.parse("chr12:65857040-65857041")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci, partialReference)
    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, partialReference, loci.result, loci.result)

    calls.collect.length should equal(1)
    calls.collect.head.alleleEvidences.length should equal(1)
    calls.collect.head.alleleEvidences.map(_.allele.ref) should equal(Seq("G"))
  }

  sparkTest("call germline variants") {
    val inputs = InputCollection(cancerWGS1Bams, tissueTypes = Seq("normal", "normal", "normal"))
    val loci = LociSet.parse("chr1,chr2,chr3")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci, partialReference)
    val calls = SomaticJoint.makeCalls(
      sc,
      inputs,
      readSets,
      Parameters.defaults,
      partialReference,
      loci.result(readSets.head.contigLengths)).collect

    calls.nonEmpty should be(true)
    calls.foreach(call => {
      call.alleleEvidences.foreach(evidence => {
        evidence.isSomaticCall should be(false)
      })
    })
  }

  sparkTest("pileupstats likelihood computation") {
    val ref = "NTCGATCGA"

    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1, qualityScores = Some(Seq.fill(8)(10))),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1, qualityScores = Some(Seq.fill(8)(20))),
      TestUtil.makeRead("TCGCTCGA", "8M", "8", 1, qualityScores = Some(Seq.fill(8)(50))),
      TestUtil.makeRead("TCGCTCGA", "8M", "8", 1, qualityScores = Some(Seq.fill(8)(50))),
      TestUtil.makeRead("TCGCTCGA", "8M", "8", 1, qualityScores = Some(Seq.fill(8)(50))),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", "8", 1, qualityScores = Some(Seq.fill(11)(30))))
    val pileups = (0 until ref.length).map(locus => Pileup(reads, "chr1", locus))

    val stats1 = PileupStats.apply(pileups(2).elements, Bases.stringToBases("G"))
    stats1.totalDepth should equal(6)
    stats1.allelicDepths should equal(Map("G" -> 6))
    assert(stats1.logLikelihoodPileup(Map("G" -> 1.0)) > stats1.logLikelihoodPileup(Map("G" -> .99, "C" -> .01)))
    assert(stats1.logLikelihoodPileup(Map("T" -> 1.0)) < stats1.logLikelihoodPileup(Map("G" -> .99, "C" -> .01)))

    val stats2 = PileupStats.apply(pileups(3).elements, Bases.stringToBases("A"))
    stats2.allelicDepths should equal(Map("A" -> 2, "C" -> 3, "ACCC" -> 1))
    assert(stats2.logLikelihoodPileup(Map("A" -> 0.5, "C" -> 0.5)) > stats2.logLikelihoodPileup(Map("A" -> 1.0)))

    // True because of the higher base qualities on the C allele:
    assert(stats2.logLikelihoodPileup(Map("C" -> 1.0)) > stats2.logLikelihoodPileup(Map("A" -> 1.0)))

    val stats3 = PileupStats.apply(pileups(4).elements, Bases.stringToBases("T"))
    stats3.totalDepth should equal(6)
    stats3.allelicDepths should equal(Map("T" -> 2)) // reads with an SNV at position 4 don't count
  }
}
