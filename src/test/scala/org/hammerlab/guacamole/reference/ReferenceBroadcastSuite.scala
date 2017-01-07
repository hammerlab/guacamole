package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.guacamole.util.{ AssertBases, GuacFunSuite }
import org.scalatest.Matchers
import org.hammerlab.guacamole.util.Bases.{ A, C, G, N, T }

class ReferenceBroadcastSuite extends GuacFunSuite with Matchers {

  val testFastaPath = resourcePath("sample.fasta")

  test("loading and broadcasting reference") {

    val reference = ReferenceBroadcast(testFastaPath, sc)
    reference.broadcastedContigs.keys.size should be(2)

    reference.broadcastedContigs.keys should contain("1")
    reference.broadcastedContigs.keys should contain("2")
  }

  test("retrieving reference sequences") {
    val reference = ReferenceBroadcast(testFastaPath, sc)

    reference.getReferenceBase("1", 0) should be(N)
    reference.getReferenceBase("1", 80) should be(C)
    reference.getReferenceBase("1", 160) should be(T)
    reference.getReferenceBase("1", 240) should be(G)
    reference.getReferenceBase("1", 320) should be(A)

    reference.getReferenceBase("2", 0) should be(N)
    reference.getReferenceBase("2", 80) should be(T)
    reference.getReferenceBase("2", 160) should be(C)
    reference.getReferenceBase("2", 240) should be(G)

    AssertBases(
      reference.getReferenceSequence("1", 80, 160),
      "CATCAAAATACCACCATCATTCTTCACAGAACTAGAAAAAACAAGGCTAAAATTCACATGGAACCAAAAAAGAGCCCACA")

    AssertBases(
      reference.getReferenceSequence("2", 240, 320),
      "GACGTTCATTCAGAATGCCACCTAACTAGGCCAGTTTTTGGACTGTATGCCAGCCTCTTTCTGCGGGATGTAATCTCAAT")

    AssertBases(
      reference.getReferenceSequence("2", 720, 800),
      "CTGATGATCGCACCTGCATAACTGCTACCAGACCTGCTAAGGGGGAGCCTGGCCCAGCCATCTCTTCTTTGTGGTCACAA")
  }
}
