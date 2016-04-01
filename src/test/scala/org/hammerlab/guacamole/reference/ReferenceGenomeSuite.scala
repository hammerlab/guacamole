package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.scalatest.{ FunSuite, Matchers }

class ReferenceGenomeSuite extends FunSuite with Matchers {

  val testFastaPath = TestUtil.testDataPath("sample.fasta")

  test("loading and broadcasting reference") {

    val reference = ReferenceGenome(testFastaPath)
    reference.contigs.keys.size should be(2)

    reference.contigs.keys should contain("1")
    reference.contigs.keys should contain("2")

  }

  test("retrieving reference sequences") {
    val reference = ReferenceGenome(testFastaPath)

    reference.getReferenceBase("1", 0) should be(Bases.N)
    reference.getReferenceBase("1", 80) should be(Bases.C)
    reference.getReferenceBase("1", 160) should be(Bases.T)
    reference.getReferenceBase("1", 240) should be(Bases.G)
    reference.getReferenceBase("1", 320) should be(Bases.A)

    reference.getReferenceBase("2", 0) should be(Bases.N)
    reference.getReferenceBase("2", 80) should be(Bases.T)
    reference.getReferenceBase("2", 160) should be(Bases.C)
    reference.getReferenceBase("2", 240) should be(Bases.G)

    TestUtil.assertBases(
      reference.getReferenceSequence("1", 80, 160),
      "CATCAAAATACCACCATCATTCTTCACAGAACTAGAAAAAACAAGGCTAAAATTCACATGGAACCAAAAAAGAGCCCACA")

    TestUtil.assertBases(
      reference.getReferenceSequence("2", 240, 320),
      "GACGTTCATTCAGAATGCCACCTAACTAGGCCAGTTTTTGGACTGTATGCCAGCCTCTTTCTGCGGGATGTAATCTCAAT")

    TestUtil.assertBases(
      reference.getReferenceSequence("2", 720, 800),
      "CTGATGATCGCACCTGCATAACTGCTACCAGACCTGCTAAGGGGGAGCCTGGCCCAGCCATCTCTTCTTTGTGGTCACAA")

  }

}
