package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.reference.ContigName
import org.hammerlab.genomics.reference.test.LocusUtil
import org.hammerlab.guacamole.util.Bases.{ A, C, G, N, T }
import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.guacamole.util.{ AssertBases, GuacFunSuite }

class ReferenceBroadcastSuite
  extends GuacFunSuite
    with LocusUtil {

  val testFastaPath = resourcePath("sample.fasta")

  test("loading and broadcasting reference") {

    val reference = ReferenceBroadcast(testFastaPath, sc)
    reference.broadcastedContigs.keys.size should be(2)

    reference.broadcastedContigs.keys should contain(ContigName("1"))
    reference.broadcastedContigs.keys should contain(ContigName("2"))
  }

  test("retrieving reference sequences") {
    val reference = ReferenceBroadcast(testFastaPath, sc)

    reference.getReferenceBase("1", 0) should ===(N)
    reference.getReferenceBase("1", 80) should ===(C)
    reference.getReferenceBase("1", 160) should ===(T)
    reference.getReferenceBase("1", 240) should ===(G)
    reference.getReferenceBase("1", 320) should ===(A)

    reference.getReferenceBase("2", 0) should ===(N)
    reference.getReferenceBase("2", 80) should ===(T)
    reference.getReferenceBase("2", 160) should ===(C)
    reference.getReferenceBase("2", 240) should ===(G)

    AssertBases(
      reference.getReferenceSequence("1", 80, 80),
      "CATCAAAATACCACCATCATTCTTCACAGAACTAGAAAAAACAAGGCTAAAATTCACATGGAACCAAAAAAGAGCCCACA")

    AssertBases(
      reference.getReferenceSequence("2", 240, 80),
      "GACGTTCATTCAGAATGCCACCTAACTAGGCCAGTTTTTGGACTGTATGCCAGCCTCTTTCTGCGGGATGTAATCTCAAT")

    AssertBases(
      reference.getReferenceSequence("2", 720, 80),
      "CTGATGATCGCACCTGCATAACTGCTACCAGACCTGCTAAGGGGGAGCCTGGCCCAGCCATCTCTTCTTTGTGGTCACAA")
  }
}
