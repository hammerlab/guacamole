package org.hammerlab.guacamole.jointcaller

import java.nio.file.Files

import org.hammerlab.guacamole.commands.SomaticJoint
import org.hammerlab.guacamole.loci.set.{LociParser, LociSet}
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}

class SomaticJointCallerSuite extends GuacFunSuite {
  val cancerWGS1Bams = Vector("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  val celsr1BAMs = Vector("normal_0.bam", "tumor_wes_2.bam", "tumor_rna_11.bam").map(
    name => TestUtil.testDataPath("cancer-wes-and-rna-celsr1/" + name))

  val hg19PartialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def hg19PartialReference = {
    ReferenceBroadcast(hg19PartialFasta, sc, partialFasta = true)
  }

  val b37Chromosome22Fasta = TestUtil.testDataPath("chr22.fa.gz")

  def b37Chromosome22Reference =
    ReferenceBroadcast(
      b37Chromosome22Fasta,
      sc,
      partialFasta = false
    )

  test("force-call a non-variant locus") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociParser("chr12:65857040")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci)
    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, hg19PartialReference, loci.result, loci.result).collect

    calls.length should equal(1)

    val evidences = calls.head.singleAlleleEvidences
    evidences.length should equal(1)

    val allele = evidences.head.allele
    allele.start should be(65857040)
    allele.ref should be("G")
  }

  test("call a somatic deletion") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociParser("chr5:82649006-82649009")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci)
    val calls = SomaticJoint.makeCalls(
      sc,
      inputs,
      readSets,
      Parameters.defaults,
      hg19PartialReference,
      loci.result,
      LociSet(),
      includeFiltered = true).collect

    calls.length should equal(1)
    calls.head.singleAlleleEvidences.length should equal(1)
    calls.head.singleAlleleEvidences.head.allele.ref should equal("TCTTTAGAAA")
    calls.head.singleAlleleEvidences.head.allele.alt should equal("T")
  }

  test("call germline variants") {
    val inputs = InputCollection(cancerWGS1Bams.take(1), tissueTypes = Vector("normal"))
    val loci = LociParser("chr1,chr2,chr3")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci)
    val calls =
      SomaticJoint.makeCalls(
        sc,
        inputs,
        readSets,
        includeFiltered = true,
        parameters = Parameters.defaults.copy(filterStrandBiasPhred = 20),
        reference = hg19PartialReference,
        loci = loci.result(readSets.contigLengths)
      )
      .collect
      .map(call => (call.contigName, call.start) -> call)
      .toMap

    calls(("chr1", 179895860)).singleAlleleEvidences.length should equal(1)
    calls(("chr1", 179895860)).bestAllele.isSomaticCall should be(false)
    calls(("chr1", 179895860)).bestAllele.isGermlineCall should be(true)
    calls(("chr1", 179895860)).bestAllele.allele.ref should equal("T")
    calls(("chr1", 179895860)).bestAllele.allele.alt should equal("C")
    calls(("chr1", 179895860)).bestAllele.allEvidences.head.annotations.get.strandBias.phredValue should equal(0)
    calls(("chr1", 179895860)).bestAllele.allEvidences.head.annotations.get.annotationsFailingFilters should equal(Seq.empty)
    calls(("chr1", 179895860)).bestAllele.annotations.get.annotationsFailingFilters should equal(Seq.empty)


//    TODO: after PR#479 this test fails as the test data no longer contains a germline variant
//    See https://github.com/hammerlab/guacamole/pull/479
//    calls(("chr1", 167190087)).bestAllele.isGermlineCall should be(true)
//    calls(("chr1", 167190087)).bestAllele.allele.ref
//    calls(("chr1", 167190087)).bestAllele.allele.alt
//    calls(("chr1", 167190087)).bestAllele.allEvidences.head.annotations.get.strandBias.phredValue > 20 should be(true)
//    calls(("chr1", 167190087)).bestAllele.allEvidences.head.annotations.get.strandBias.isFiltered should be(true)
//    calls(("chr1", 167190087)).bestAllele.failingFilterNames.contains("STRAND_BIAS") should be(true)
  }

  test("don't call variants with N as the reference base") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociParser("chr12:65857030-65857080")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci)
    val emptyPartialReference = ReferenceBroadcast(
      Map("chr12" -> MapBackedReferenceSequence(500000000, sc.broadcast(Map.empty))))
    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, emptyPartialReference, loci.result, loci.result)

    calls.collect.length should equal(0)
  }

  test("call a somatic variant using RNA evidence") {
    val parameters = Parameters.defaults.copy(somaticNegativeLog10VariantPriorWithRnaEvidence = 1)

    val loci = LociParser("chr22:46931058-46931079")
    val inputsWithRNA = InputCollection(celsr1BAMs, analytes = Vector("dna", "dna", "rna"))
    val callsWithRNA =
      SomaticJoint.makeCalls(
        sc,
        inputsWithRNA,
        SomaticJoint.inputsToReadSets(sc, inputsWithRNA, loci),
        parameters,
        b37Chromosome22Reference,
        loci.result
      )
      .collect
      .filter(_.bestAllele.isCall)

    val inputsWithoutRNA = InputCollection(celsr1BAMs.take(2), analytes = Vector("dna", "dna"))
    val callsWithoutRNA = SomaticJoint.makeCalls(
      sc,
      inputsWithoutRNA,
      SomaticJoint.inputsToReadSets(sc, inputsWithoutRNA, loci),
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
