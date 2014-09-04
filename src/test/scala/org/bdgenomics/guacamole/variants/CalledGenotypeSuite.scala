package org.bdgenomics.guacamole.variants

import org.apache.spark.SparkEnv
import org.bdgenomics.guacamole.{ Bases, TestUtil }
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.scalatest.FunSuite

class CalledGenotypeSuite extends FunSuite with SparkFunSuite {

  test("serializing called genotype") {
    val gt = CalledGenotype("sample",
      "chr1",
      123456789123L,
      Bases.T,
      Seq(Bases.A),
      evidence = GenotypeEvidence(0.99, 15, 10, 10, 5, 60, 30))

    val serialized = TestUtil.serialize(gt)
    val deserialized = TestUtil.deserialize[CalledGenotype](serialized)

    assert(gt === deserialized)
  }

  test("serializing called somatic genotype") {

    val sgt = new CalledSomaticGenotype("sample",
      "chr1",
      123456789123L,
      Bases.T,
      Seq(Bases.A),
      0.99 / 0.01,
      tumorEvidence = GenotypeEvidence(0.99, 15, 10, 10, 5, 60, 30),
      normalEvidence = GenotypeEvidence(0.01, 17, 0, 10, 0, 60, 30))

    val serialized = TestUtil.serialize(sgt)
    val deserialized = TestUtil.deserialize[CalledSomaticGenotype](serialized)

    assert(sgt === deserialized)

  }

  sparkTest("serializing multi-base called somatic genotype") {

    val serializer = SparkEnv.get.serializer.newInstance()

    val sgt = new CalledSomaticGenotype("sample",
      "chr1",
      123456789123L,
      Bases.T,
      Seq(Bases.T, Bases.A, Bases.T),
      0.99 / 0.01,
      tumorEvidence = GenotypeEvidence(0.99, 15, 10, 10, 5, 60, 30),
      normalEvidence = GenotypeEvidence(0.01, 17, 0, 10, 0, 60, 30))

    val serialized = serializer.serialize(sgt)
    val deserialized = serializer.deserialize[CalledSomaticGenotype](serialized)

    assert(sgt === deserialized)

  }

}