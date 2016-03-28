package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.LociSet
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.scalatest.Matchers

class SomaticJointCallerSuite extends GuacFunSuite with Matchers {
  val cancerWGS1Bams = Vector("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  val celsr1BAMs = Vector("normal_0.bam", "tumor_wes_2.bam", "tumor_rna_11.bam").map(
    name => TestUtil.testDataPath("cancer-wes-and-rna-celsr1/" + name))

  val hg19PartialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def hg19PartialReference = {
    ReferenceBroadcast(hg19PartialFasta, sc, partialFasta = true)
  }

  val b37Chromosome22Fasta = TestUtil.testDataPath("chr22.fa.gz")
  def b37Chromosome22Reference = {
    ReferenceBroadcast(b37Chromosome22Fasta, sc, partialFasta = false)
  }

  sparkTest("call a somatic variant") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociSet.parse("chr12:65857040-65857041")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci, hg19PartialReference)
    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, hg19PartialReference, loci.result, loci.result).collect

    calls.length should equal(1)
    calls.head.alleleEvidences.length should equal(1)
    calls.head.alleleEvidences.map(_.allele.ref) should equal(Seq("G"))
  }

  sparkTest("call a somatic deletion") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociSet.parse("chr5:82649006-82649009")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci, hg19PartialReference)
    val calls = SomaticJoint.makeCalls(
      sc,
      inputs,
      readSets,
      Parameters.defaults,
      hg19PartialReference,
      loci.result,
      LociSet.empty,
      includeFiltered = true).collect

    calls.length should equal(1)
    calls.head.alleleEvidences.length should equal(1)
    calls.head.alleleEvidences.head.allele.ref should equal("TCTTTAGAAA")
    calls.head.alleleEvidences.head.allele.alt should equal("T")
  }

  sparkTest("call germline variants") {
    val inputs = InputCollection(cancerWGS1Bams, tissueTypes = Vector("normal", "normal", "normal"))
    val loci = LociSet.parse("chr1,chr2,chr3")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci, hg19PartialReference)
    val calls = SomaticJoint.makeCalls(
      sc,
      inputs,
      readSets,
      Parameters.defaults,
      hg19PartialReference,
      loci.result(readSets.head.contigLengths)).collect

    calls.nonEmpty should be(true)
    calls.foreach(call => {
      call.alleleEvidences.foreach(evidence => {
        evidence.isSomaticCall should be(false)
      })
    })
  }

  sparkTest("don't call variants with N as the reference base") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociSet.parse("chr12:65857030-65857080")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci, hg19PartialReference)
    val emptyPartialReference = ReferenceBroadcast(
      Map("chr12" -> MapBackedReferenceSequence(500000000, sc.broadcast(Map.empty))))
    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, emptyPartialReference, loci.result, loci.result)

    calls.collect.length should equal(0)
  }

  sparkTest("call a somatic variant using RNA evidence") {
    val parameters = Parameters.defaults.copy(somaticNegativeLog10VariantPriorWithRnaEvidence = 1)

    val loci = LociSet.parse("chr22:46931058-46931079")
    val inputsWithRNA = InputCollection(celsr1BAMs, analytes = Vector("dna", "dna", "rna"))
    val callsWithRNA = SomaticJoint.makeCalls(
      sc,
      inputsWithRNA,
      SomaticJoint.inputsToReadSets(sc, inputsWithRNA, loci, b37Chromosome22Reference),
      parameters,
      b37Chromosome22Reference,
      loci.result).collect.filter(_.bestAllele.isCall)

    val inputsWithoutRNA = InputCollection(celsr1BAMs.take(2), analytes = Vector("dna", "dna"))
    val callsWithoutRNA = SomaticJoint.makeCalls(
      sc,
      inputsWithoutRNA,
      SomaticJoint.inputsToReadSets(sc, inputsWithoutRNA, loci, b37Chromosome22Reference),
      parameters,
      b37Chromosome22Reference,
      loci.result).collect.filter(_.bestAllele.isCall)

    Map("with rna" -> callsWithRNA, "without rna" -> callsWithoutRNA).foreach({
      case (description, calls) => {
        withClue("germline variant %s".format(description)) {
          // There should be a germline homozygous call at 22:46931077 in one based, which is 22:46931076 in zero based.
          val filtered46931076 = calls.filter(call => call.start == 46931076 && call.end == 46931077)
          filtered46931076.length should be(1)
          filtered46931076.head.bestAllele.isGermlineCall should be(true)
          filtered46931076.head.bestAllele.allele.ref should equal("G")
          filtered46931076.head.bestAllele.allele.alt should equal("C")
          filtered46931076.head.bestAllele.germlineAlleles should equal("C", "C")
        }
      }
    })

    // RNA should enable a call G->A call at 22:46931062 in one based, which is 22:46931061 in zero based.
    callsWithoutRNA.exists(call => call.start == 46931061 && call.end == 46931062) should be(false)
    val filtered46931061 = callsWithRNA.filter(call => call.start == 46931061 && call.end == 46931062)
    filtered46931061.length should be(1)
    filtered46931061.head.bestAllele.isSomaticCall should be(true)
    filtered46931061.head.bestAllele.allele.ref should equal("G")
    filtered46931061.head.bestAllele.allele.alt should equal("A")
    filtered46931061.head.bestAllele.tumorDNAPooledEvidence.allelicDepths.toSet should equal(Set("G" -> 90, "A" -> 2))
    filtered46931061.head.bestAllele.normalDNAPooledEvidence.allelicDepths.toSet should equal(Set("G" -> 51))
  }

}
