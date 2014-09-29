package org.bdgenomics.guacamole.variants

import org.bdgenomics.guacamole.{TestUtil, Bases}
import org.scalatest.{Matchers, FunSuite}


class AlleleSuite extends FunSuite with Matchers {

  test("isVariant") {
    val mismatch = Allele(Seq(Bases.T), Seq(Bases.A))
    mismatch.isVariant should be(true)

    val reference = Allele(Seq(Bases.T), Seq(Bases.T))
    reference.isVariant should be(false)

    val deletion = Allele(Seq(Bases.T, Bases.T, Bases.T), Seq(Bases.T))
    deletion.isVariant should be(true)

    val insertion = Allele(Seq(Bases.T), Seq(Bases.T, Bases.A, Bases.A))
    insertion.isVariant should be(true)


  }

  test("serializing allele") {
    val a1 = Allele(Seq(Bases.T), Seq(Bases.A))
    val a1Serialized = TestUtil.serialize(a1)
    val a1Deserialized = TestUtil.deserialize[Allele](a1Serialized)

    assert(a1 === a1Deserialized)

    val a2 = Allele(Seq(Bases.T, Bases.T, Bases.C), Seq(Bases.A))
    val a2Serialized = TestUtil.serialize(a2)
    val a2Deserialized = TestUtil.deserialize[Allele](a2Serialized)

    assert(a2 === a2Deserialized)
  }

}
