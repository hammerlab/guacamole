package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.LociSet
import org.hammerlab.guacamole.reference.{ MapBackedReferenceSequence, ReferenceGenome }
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.scalatest.Matchers

class SomaticJointCallerSuite extends GuacFunSuite with Matchers {
  val cancerWGS1Bams = Vector("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  val partialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def partialReference = {
    ReferenceGenome(partialFasta, partialFasta = true)
  }

  sparkTest("call a somatic variant") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociSet.parse("chr12:65857040")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci)
    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, partialReference, loci.result, loci.result).collect

    calls.length should equal(1)
    calls.head.alleleEvidences.length should equal(1)
    calls.head.alleleEvidences.map(_.allele.ref) should equal(Seq("G"))
  }

  sparkTest("call a somatic deletion") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociSet.parse("chr5:82649006-82649009")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci)
    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, partialReference, loci.result, LociSet.empty).collect

    calls.length should equal(1)
    calls.head.alleleEvidences.length should equal(1)
    calls.head.alleleEvidences.head.allele.ref should equal("TCTTTAGAAA")
    calls.head.alleleEvidences.head.allele.alt should equal("T")
  }

  sparkTest("call germline variants") {
    val inputs = InputCollection(cancerWGS1Bams, tissueTypes = Vector("normal", "normal", "normal"))
    val loci = LociSet.parse("chr1,chr2,chr3")
    val readSets = SomaticJoint.inputsToReadSets(sc, inputs, loci)
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

  sparkTest("don't call variants with N as the reference base") {
    val inputs = InputCollection(cancerWGS1Bams)
    val loci = LociSet.parse("chr12:65857030-65857080")
    val emptyPartialReference =
      ReferenceGenome(
        Map(
          "chr12" -> MapBackedReferenceSequence(500000000, Nil)
        )
      )

    val readSets =
      SomaticJoint.inputsToReadSets(
        sc,
        inputs,
        loci
      )

    val calls = SomaticJoint.makeCalls(
      sc, inputs, readSets, Parameters.defaults, emptyPartialReference, loci.result, loci.result)

    calls.collect.length should equal(0)
  }
}
