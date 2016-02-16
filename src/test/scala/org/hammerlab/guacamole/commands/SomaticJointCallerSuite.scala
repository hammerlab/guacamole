package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.commands.jointcaller._
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.reference.ReferenceBroadcast.ArrayBackedReferenceSequence
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
import org.hammerlab.guacamole.{Bases, LociSet}
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
    ReadSubsequence.ofFixedReferenceLength(pileups(1).head, Bases.stringToBases("CGATCGA")).get.sequence should equal("CGACCCTCGA")
    ReadSubsequence.ofFixedReferenceLength(pileups(2).head, Bases.stringToBases("CGATCGA")).get.sequence should equal("CGAGCGA")
  }

  sparkTest("read subsequence ofNextAltAllele") {
    val ref = ArrayBackedReferenceSequence(sc, "NTCGATCGA")
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1), // no variant
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", "8", 1), // insertion
      TestUtil.makeRead("TCGAGCGA", "8M", "8", 1) // snv
    )
    val pileups = reads.map(read => Pileup(Seq(read), "chr1", 1))

    ReadSubsequence.ofNextAltAllele(pileups(0).elements(0), ref) should equal(None)

    ReadSubsequence.ofNextAltAllele(
      pileups(2).atGreaterLocus(4, ref(4), Iterator.empty).elements(0), ref).get.sequence should equal("G")

    ReadSubsequence.ofNextAltAllele(
      pileups(1).atGreaterLocus(3, ref(3), Iterator.empty).elements(0), ref).get.sequence should equal("ACCC")
  }

  sparkTest("gathering possible alleles") {
    val inputs = InputCollection.parseMultiple(cancerWGS1Bams)
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
    val inputs = InputCollection.parseMultiple(cancerWGS1Bams)
    val loci = LociSet.parse("chr12:65857040-65857041")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci, partialReference)
    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, partialReference, loci.result, loci.result)

    calls.length should equal(1)
    calls.head.alleleEvidences.length should equal(1)
    calls.head.alleleEvidences.map(_.allele.ref) should equal(Seq("G"))
  }
}
